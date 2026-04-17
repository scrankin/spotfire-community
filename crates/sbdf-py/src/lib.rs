use std::io;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sbdf::{
    BoolArray, ColumnMetadata, ColumnProperties, ColumnSlice, DoubleArray, EncodedValue,
    FileHeader, FloatArray, IntArray, LongArray, Metadata, Object, SbdfWriter, SectionId,
    StringArray, TableMetadata, TableSlice, ValueType,
};

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

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

fn flush_section<F>(output: &mut Vec<u8>, build: F) -> Result<(), sbdf::SbdfError>
where
    F: FnOnce(&mut SbdfWriter) -> Result<(), sbdf::SbdfError>,
{
    let mut buf: Vec<u8> = Vec::new();
    {
        let cursor = io::Cursor::new(&mut buf);
        let mut writer = SbdfWriter::new(cursor);
        build(&mut writer)?;
    }
    output.extend_from_slice(&buf);
    Ok(())
}

/// Convert CSV bytes to SBDF bytes.
///
/// Args:
///     csv_bytes: Raw UTF-8 encoded CSV data (with header row).
///     chunk_size: Number of rows per SBDF table slice (default 10 000).
///     sample_rows: Number of rows to sample for column type inference (default 1 000).
///
/// Returns:
///     SBDF file contents as bytes.
///
/// Raises:
///     ValueError: If the CSV cannot be parsed or the SBDF cannot be written.
#[pyfunction]
#[pyo3(signature = (csv_bytes, chunk_size=10_000, sample_rows=1_000))]
fn csv_to_sbdf(
    csv_bytes: &[u8],
    chunk_size: usize,
    sample_rows: usize,
) -> PyResult<Vec<u8>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(csv_bytes);

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| PyValueError::new_err(format!("CSV header error: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();
    let num_cols = headers.len();

    // Phase 1: sample rows for type inference.
    let mut sampled: Vec<Vec<String>> = Vec::new();
    let mut records = reader.into_records();

    for _ in 0..sample_rows {
        match records.next() {
            Some(Ok(record)) => {
                let row: Vec<String> = (0..num_cols)
                    .map(|i| record.get(i).unwrap_or("").to_string())
                    .collect();
                sampled.push(row);
            }
            Some(Err(e)) => {
                return Err(PyValueError::new_err(format!("CSV parse error: {e}")));
            }
            None => break,
        }
    }

    let column_types: Vec<ValueType> = (0..num_cols)
        .map(|col_idx| {
            let col_values: Vec<String> = sampled.iter().map(|row| row[col_idx].clone()).collect();
            infer_type(&col_values)
        })
        .collect();

    // Write FileHeader + TableMetadata.
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

    let mut output: Vec<u8> = Vec::new();

    flush_section(&mut output, |w| {
        w.write_section_id(SectionId::FileHeader)?;
        w.write_file_header(&file_header)?;
        w.write_section_id(SectionId::TableMetadata)?;
        w.write_table_metadata(&table_metadata)?;
        Ok(())
    })
    .map_err(|e| PyValueError::new_err(format!("SBDF write error: {e}")))?;

    // Phase 2: stream table slices.
    // Use a VecDeque so sampled rows are drained in chunk_size batches even when
    // sample_rows > chunk_size, rather than emitting an oversized first slice.
    let mut queue: std::collections::VecDeque<Vec<String>> = sampled.into_iter().collect();

    loop {
        while queue.len() < chunk_size {
            match records.next() {
                Some(Ok(record)) => {
                    let row: Vec<String> = (0..num_cols)
                        .map(|i| record.get(i).unwrap_or("").to_string())
                        .collect();
                    queue.push_back(row);
                }
                Some(Err(e)) => {
                    return Err(PyValueError::new_err(format!("CSV parse error: {e}")));
                }
                None => break,
            }
        }

        if queue.is_empty() {
            break;
        }

        let take = chunk_size.min(queue.len());
        let current_chunk: Vec<Vec<String>> = queue.drain(..take).collect();
        let at_eof = current_chunk.len() < chunk_size;

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

        flush_section(&mut output, |w| {
            w.write_section_id(SectionId::TableSlice)?;
            w.write_table_slice(&slice)?;
            Ok(())
        })
        .map_err(|e| PyValueError::new_err(format!("SBDF write error: {e}")))?;

        if at_eof {
            break;
        }
    }

    flush_section(&mut output, |w| {
        w.write_section_id(SectionId::TableEnd)?;
        Ok(())
    })
    .map_err(|e| PyValueError::new_err(format!("SBDF write error: {e}")))?;

    Ok(output)
}

#[pymodule]
fn _sbdf(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(csv_to_sbdf, m)?)?;
    Ok(())
}
