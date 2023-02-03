use crate::Frame;

pub fn resp_ok() -> Frame {
    Frame::Simple("OK".to_string())
}

pub fn resp_str(val: &str) -> Frame {
    Frame::Simple(val.to_string())
}

pub fn resp_invalid_arguments() -> Frame {
    Frame::Error("Invalid arguments".to_string())
}