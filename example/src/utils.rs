use std::num::ParseIntError;

#[derive(Debug)]
pub enum NumberConversionError {
    InvalidLength(String),
}

// Converts a `u64` number to a `Vec<u8>` byte representation.
pub fn to_bytes(num: u64) -> Vec<u8> {
    num.to_le_bytes().to_vec()
}

// Converts a `Vec<u8>` to a `u64` number.
pub fn from_bytes(bytes: &[u8]) -> Result<u64, NumberConversionError> {
    match bytes.len() {
        8 => Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            NumberConversionError::InvalidLength(format!(
                "Invalid payload length: expected 8 bytes, got {}",
                bytes.len()
            ))
        })?)),
        _ => Err(NumberConversionError::InvalidLength(format!(
            "Invalid payload length: expected 8 bytes, got {}",
            bytes.len()
        ))),
    }
}

pub fn to_string(num: u64) -> String {
    num.to_string()
}

pub fn from_string(num: &str) -> Result<u64, ParseIntError> {
    u64::from_str_radix(num, 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        let num: u64 = 42;
        let bytes = to_bytes(num);
        assert_eq!(bytes, num.to_le_bytes().to_vec());
    }

    #[test]
    fn test_from_bytes() {
        let num: u64 = 42;
        let bytes = num.to_le_bytes().to_vec();
        let result = from_bytes(&bytes);
        assert_eq!(result.unwrap(), num);
    }

    #[test]
    fn test_invalid_length() {
        let bytes = vec![1, 2, 3]; // Length is not 8 bytes for a u64 number
        let result = from_bytes(&bytes);
        assert!(result.is_err());
    }
}
