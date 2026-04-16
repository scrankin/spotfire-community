use std::io::{self, Read, Write};

use clap::Parser;
use sbdf::{
    BoolArray, ColumnMetadata, ColumnProperties, ColumnSlice, DoubleArray, EncodedValue,
    FileHeader, FloatArray, IntArray, LongArray, Metadata, Object, SbdfWriter, SectionId,
    StringArray, TableMetadata, TableSlice, ValueType,
};

#[derive(Parser)]
#[command(name = "tabular-to-sbdf")]
#[command(about = "Convert CSV to Spotfire Binary Data Format (SBDF)")]
struct Cli {
    /// Input format (only "csv" is currently supported)
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

    /// Number of rows to sample for type inference
    #[arg(long, default_value = "1000")]
    sample_rows: usize,

    /// Emit progress logs to stderr
    #[arg(long)]
    verbose: bool,
}

/// Parse a strict boolean value (true/false, case-insensitive).
fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
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

    if non_empty.iter().all(|v| parse_bool(v).is_some()) {
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

/// Build a column slice from string values, setting the IsInvalid property for
/// any cells that are empty or fail to parse as the target type.
fn build_column_slice(values: &[String], ty: ValueType) -> ColumnSlice {
    let mut invalid_flags: Vec<bool> = Vec::with_capacity(values.len());

    let array = match ty {
        ValueType::Bool => {
            let arr: Vec<bool> = values
                .iter()
                .map(|s| match parse_bool(s) {
                    Some(v) => {
                        invalid_flags.push(false);
                        v
                    }
                    None => {
                        invalid_flags.push(true);
                        false
                    }
                })
                .collect();
            Object::BoolArray(BoolArray(arr.into_boxed_slice()))
        }
        ValueType::Int => {
            let arr: Vec<i32> = values
                .iter()
                .map(|s| match s.parse::<i32>() {
                    Ok(v) => {
                        invalid_flags.push(false);
                        v
                    }
                    Err(_) => {
                        invalid_flags.push(true);
                        0
                    }
                })
                .collect();
            Object::IntArray(IntArray(arr.into_boxed_slice()))
        }
        ValueType::Long => {
            let arr: Vec<i64> = values
                .iter()
                .map(|s| match s.parse::<i64>() {
                    Ok(v) => {
                        invalid_flags.push(false);
                        v
                    }
                    Err(_) => {
                        invalid_flags.push(true);
                        0
                    }
                })
                .collect();
            Object::LongArray(LongArray(arr.into_boxed_slice()))
        }
        ValueType::Double => {
            let arr: Vec<f64> = values
                .iter()
                .map(|s| match s.parse::<f64>() {
                    Ok(v) => {
                        invalid_flags.push(false);
                        v
                    }
                    Err(_) => {
                        invalid_flags.push(true);
                        0.0
                    }
                })
                .collect();
            Object::DoubleArray(DoubleArray(arr.into_boxed_slice()))
        }
        ValueType::Float => {
            let arr: Vec<f32> = values
                .iter()
                .map(|s| match s.parse::<f32>() {
                    Ok(v) => {
                        invalid_flags.push(false);
                        v
                    }
                    Err(_) => {
                        invalid_flags.push(true);
                        0.0
                    }
                })
                .collect();
            Object::FloatArray(FloatArray(arr.into_boxed_slice()))
        }
        _ => {
            // Empty strings are null; non-empty are valid values.
            for s in values {
                invalid_flags.push(s.is_empty());
            }
            Object::StringArray(StringArray(values.to_vec().into_boxed_slice()))
        }
    };

    let is_invalid = if invalid_flags.iter().any(|f| *f) {
        Some(BoolArray(invalid_flags.into_boxed_slice()).encode_to_bit_array())
    } else {
        None
    };

    ColumnSlice {
        values: EncodedValue::Plain(array),
        properties: ColumnProperties {
            is_invalid,
            error_code: None,
            has_replaced_value: None,
            other: Box::new([]),
        },
    }
}

/// Write a Sbdf section to output via a short-lived buffer. Keeps memory bounded
/// to the size of a single section regardless of total file size.
fn flush_section<F>(output: &mut dyn Write, build: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&mut SbdfWriter) -> Result<(), sbdf::SbdfError>,
{
    let mut buf: Vec<u8> = Vec::new();
    {
        let cursor = io::Cursor::new(&mut buf);
        let mut writer = SbdfWriter::new(cursor);
        build(&mut writer)?;
    }
    output.write_all(&buf)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.format != "csv" {
        eprintln!("Only CSV format is currently supported (--format csv).");
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

    // Phase 1: Sample rows for type inference.
    let mut sample_rows: Vec<Vec<String>> = Vec::new();
    let mut records = reader.into_records();

    for _ in 0..cli.sample_rows {
        match records.next() {
            Some(Ok(record)) => {
                let row: Vec<String> = (0..num_cols)
                    .map(|i| record.get(i).unwrap_or("").to_string())
                    .collect();
                sample_rows.push(row);
            }
            Some(Err(e)) => {
                if cli.verbose {
                    eprintln!("Warning: skipping malformed row: {e}");
                }
            }
            None => break,
        }
    }

    let column_types: Vec<ValueType> = (0..num_cols)
        .map(|col_idx| {
            let col_values: Vec<String> =
                sample_rows.iter().map(|row| row[col_idx].clone()).collect();
            infer_type(&col_values)
        })
        .collect();

    if cli.verbose {
        eprintln!(
            "Inferred types: {}",
            headers
                .iter()
                .zip(column_types.iter())
                .map(|(h, t)| format!("{h}:{t:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Set up output sink.
    let mut output: Box<dyn Write> = match &cli.output {
        Some(path) => Box::new(std::fs::File::create(path)?),
        None => Box::new(io::stdout()),
    };

    // Write FileHeader + TableMetadata once up front.
    let file_header = FileHeader {
        major_version: 1,
        minor_version: 0,
    };

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

    flush_section(&mut *output, |w| {
        w.write_section_id(SectionId::FileHeader)?;
        w.write_file_header(&file_header)?;
        w.write_section_id(SectionId::TableMetadata)?;
        w.write_table_metadata(&table_metadata)?;
        Ok(())
    })?;

    // Phase 2: Stream table slices — one SBDF section written per chunk,
    // then the chunk is dropped from memory before reading the next.
    let chunk_size = cli.chunk_size;
    let mut current_chunk = sample_rows;
    let mut slice_count: usize = 0;
    let mut total_rows: usize = 0;

    loop {
        while current_chunk.len() < chunk_size {
            match records.next() {
                Some(Ok(record)) => {
                    let row: Vec<String> = (0..num_cols)
                        .map(|i| record.get(i).unwrap_or("").to_string())
                        .collect();
                    current_chunk.push(row);
                }
                Some(Err(e)) => {
                    if cli.verbose {
                        eprintln!("Warning: skipping malformed row: {e}");
                    }
                }
                None => break,
            }
        }

        if current_chunk.is_empty() {
            break;
        }

        let rows_in_slice = current_chunk.len();
        let at_eof = rows_in_slice < chunk_size;

        let column_slices: Vec<ColumnSlice> = (0..num_cols)
            .map(|col_idx| {
                let col_values: Vec<String> =
                    current_chunk.iter().map(|row| row[col_idx].clone()).collect();
                build_column_slice(&col_values, column_types[col_idx])
            })
            .collect();

        let slice = TableSlice {
            column_slices: column_slices.into_boxed_slice(),
        };

        flush_section(&mut *output, |w| {
            w.write_section_id(SectionId::TableSlice)?;
            w.write_table_slice(&slice)?;
            Ok(())
        })?;

        slice_count += 1;
        total_rows += rows_in_slice;

        if cli.verbose {
            eprintln!(
                "Wrote table slice #{slice_count} ({rows_in_slice} rows, {total_rows} total)"
            );
        }

        if at_eof {
            break;
        }

        current_chunk = Vec::new();
    }

    // Final TableEnd marker.
    flush_section(&mut *output, |w| {
        w.write_section_id(SectionId::TableEnd)?;
        Ok(())
    })?;

    output.flush()?;

    if cli.verbose {
        eprintln!("SBDF output complete: {slice_count} slices, {total_rows} rows");
    }

    Ok(())
}
