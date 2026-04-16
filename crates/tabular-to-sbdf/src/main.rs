use std::io::{self, Read, Write};

use clap::Parser;
use sbdf::{
    BoolArray, ColumnMetadata, ColumnProperties, ColumnSlice, DoubleArray, EncodedValue,
    FileHeader, FloatArray, IntArray, LongArray, Metadata, Object, Sbdf, StringArray,
    TableMetadata, TableSlice, ValueType,
};

#[derive(Parser)]
#[command(name = "tabular-to-sbdf")]
#[command(about = "Convert CSV to Spotfire Binary Data Format (SBDF)")]
struct Cli {
    /// Input format (csv)
    #[arg(long, default_value = "csv")]
    format: String,

    /// Input file path (default: stdin)
    #[arg(long)]
    input: Option<String>,

    /// Output file path (default: stdout)
    #[arg(long)]
    output: Option<String>,

    /// Rows per SBDF table slice for chunked writing
    #[arg(long, default_value = "10000")]
    chunk_size: usize,

    /// Number of rows to sample for type inference (CSV only)
    #[arg(long, default_value = "1000")]
    sample_rows: usize,
}

/// Infer the best ValueType for a column of string values.
fn infer_type(values: &[String]) -> ValueType {
    let non_empty: Vec<&str> = values
        .iter()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .collect();
    if non_empty.is_empty() {
        return ValueType::String;
    }

    if non_empty
        .iter()
        .all(|v| matches!(v.to_lowercase().as_str(), "true" | "false"))
    {
        return ValueType::Bool;
    }

    if non_empty.iter().all(|v| v.parse::<i32>().is_ok()) {
        return ValueType::Int;
    }

    if non_empty.iter().all(|v| v.parse::<i64>().is_ok()) {
        return ValueType::Long;
    }

    if non_empty.iter().all(|v| v.parse::<f64>().is_ok()) {
        return ValueType::Double;
    }

    ValueType::String
}

/// Build a column slice from a vector of string values and a target type.
fn build_column_slice(values: &[String], ty: ValueType) -> ColumnSlice {
    let array = match ty {
        ValueType::Bool => {
            let arr: Vec<bool> = values
                .iter()
                .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1"))
                .collect();
            Object::BoolArray(BoolArray(arr.into_boxed_slice()))
        }
        ValueType::Int => {
            let arr: Vec<i32> = values.iter().map(|s| s.parse().unwrap_or(0)).collect();
            Object::IntArray(IntArray(arr.into_boxed_slice()))
        }
        ValueType::Long => {
            let arr: Vec<i64> = values.iter().map(|s| s.parse().unwrap_or(0)).collect();
            Object::LongArray(LongArray(arr.into_boxed_slice()))
        }
        ValueType::Double => {
            let arr: Vec<f64> = values
                .iter()
                .map(|s| s.parse().unwrap_or(f64::NAN))
                .collect();
            Object::DoubleArray(DoubleArray(arr.into_boxed_slice()))
        }
        ValueType::Float => {
            let arr: Vec<f32> = values
                .iter()
                .map(|s| s.parse().unwrap_or(f32::NAN))
                .collect();
            Object::FloatArray(FloatArray(arr.into_boxed_slice()))
        }
        _ => {
            let arr: Vec<String> = values.to_vec();
            Object::StringArray(StringArray(arr.into_boxed_slice()))
        }
    };

    ColumnSlice {
        values: EncodedValue::Plain(array),
        properties: ColumnProperties {
            is_invalid: None,
            error_code: None,
            has_replaced_value: None,
            other: Box::new([]),
        },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.format != "csv" {
        eprintln!("Only CSV format is currently supported. Parquet support coming soon.");
        std::process::exit(1);
    }

    let input: Box<dyn Read> = match &cli.input {
        Some(path) => Box::new(std::fs::File::open(path)?),
        None => Box::new(io::stdin()),
    };

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(input);

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
    let num_cols = headers.len();

    // Phase 1: Read sample rows for type inference
    let mut sample_rows: Vec<Vec<String>> = Vec::new();
    let mut remaining_records = reader.into_records();

    for _ in 0..cli.sample_rows {
        match remaining_records.next() {
            Some(Ok(record)) => {
                let row: Vec<String> = (0..num_cols)
                    .map(|i| record.get(i).unwrap_or("").to_string())
                    .collect();
                sample_rows.push(row);
            }
            Some(Err(e)) => {
                eprintln!("Warning: skipping malformed row: {e}");
            }
            None => break,
        }
    }

    // Infer column types from sample
    let column_types: Vec<ValueType> = (0..num_cols)
        .map(|col_idx| {
            let col_values: Vec<String> =
                sample_rows.iter().map(|row| row[col_idx].clone()).collect();
            infer_type(&col_values)
        })
        .collect();

    eprintln!(
        "Inferred types: {}",
        headers
            .iter()
            .zip(column_types.iter())
            .map(|(h, t)| format!("{h}:{t:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Build column metadata
    let columns: Vec<ColumnMetadata> = headers
        .iter()
        .zip(column_types.iter())
        .map(|(name, ty)| ColumnMetadata {
            name: name.clone(),
            ty: *ty,
            other: Box::new([]),
        })
        .collect();

    let table_metadata = TableMetadata {
        metadata: Box::new([Metadata {
            name: "TableColumns".to_string(),
            value: Object::Int(num_cols as i32),
            default_value: None,
        }]),
        columns: columns.into_boxed_slice(),
    };

    let file_header = FileHeader {
        major_version: 1,
        minor_version: 0,
    };

    // Build table slices from sample rows + remaining rows
    let chunk_size = cli.chunk_size;
    let mut all_slices: Vec<TableSlice> = Vec::new();
    let mut current_chunk = sample_rows;

    loop {
        // Fill the current chunk up to chunk_size
        while current_chunk.len() < chunk_size {
            match remaining_records.next() {
                Some(Ok(record)) => {
                    let row: Vec<String> = (0..num_cols)
                        .map(|i| record.get(i).unwrap_or("").to_string())
                        .collect();
                    current_chunk.push(row);
                }
                Some(Err(e)) => {
                    eprintln!("Warning: skipping malformed row: {e}");
                }
                None => break,
            }
        }

        if current_chunk.is_empty() {
            break;
        }

        let rows_in_slice = current_chunk.len();
        let at_eof = rows_in_slice < chunk_size;

        // Build column slices for this chunk
        let column_slices: Vec<ColumnSlice> = (0..num_cols)
            .map(|col_idx| {
                let col_values: Vec<String> =
                    current_chunk.iter().map(|row| row[col_idx].clone()).collect();
                build_column_slice(&col_values, column_types[col_idx])
            })
            .collect();

        all_slices.push(TableSlice {
            column_slices: column_slices.into_boxed_slice(),
        });

        eprintln!(
            "Built table slice with {rows_in_slice} rows (total slices: {})",
            all_slices.len()
        );

        if at_eof {
            break;
        }

        current_chunk = Vec::new();
    }

    // Serialize the complete SBDF
    let sbdf = Sbdf {
        file_header,
        table_metadata,
        table_slices: all_slices.into_boxed_slice(),
    };

    let bytes = sbdf.to_bytes()?;

    // Write output
    let mut output: Box<dyn Write> = match &cli.output {
        Some(path) => Box::new(std::fs::File::create(path)?),
        None => Box::new(io::stdout()),
    };

    output.write_all(&bytes)?;
    output.flush()?;

    eprintln!("Wrote {} bytes of SBDF data", bytes.len());

    Ok(())
}
