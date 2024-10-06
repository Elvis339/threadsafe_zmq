use std::fmt;
use zmq;

#[derive(Debug)]
pub enum ChannelPairError {
    Zmq(zmq::Error),
    ChannelError(String),
    ConfigurationError(String),
    Other(String),
}

impl From<zmq::Error> for ChannelPairError {
    fn from(error: zmq::Error) -> Self {
        ChannelPairError::Zmq(error)
    }
}

impl fmt::Display for ChannelPairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelPairError::Zmq(e) => write!(f, "ZeroMQ Error: {}", e),
            ChannelPairError::ChannelError(msg) => write!(f, "Channel Error: {}", msg),
            ChannelPairError::ConfigurationError(msg) => write!(f, "Configuration Error: {}", msg),
            ChannelPairError::Other(msg) => write!(f, "Other Error: {}", msg),
        }
    }
}

impl std::error::Error for ChannelPairError {}
