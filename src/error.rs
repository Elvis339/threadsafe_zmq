use std::fmt;

#[derive(Debug)]
pub enum ChannelPairError {
    /// An error from the underlying ZeroMQ library.
    ///
    /// This wraps [`zmq::Error`] and includes socket creation, binding,
    /// connecting, and message send/receive errors.
    Zmq(zmq::Error),

    /// The channel has been disconnected.
    ///
    /// This occurs when trying to send or receive on a channel after
    /// the `ChannelPair` has been shut down or dropped.
    ChannelDisconnected(String),

    /// A channel operation failed.
    ///
    /// This is a general channel error that doesn't fit other categories.
    #[deprecated(since = "2.0.0", note = "use ChannelDisconnected instead")]
    ChannelError(String),

    /// A configuration error occurred.
    ///
    /// This can occur when setting socket options or other configuration fails.
    ConfigurationError(String),

    /// Any other error.
    ///
    /// Used for errors that don't fit other categories, such as thread
    /// spawning failures.
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
            ChannelPairError::Zmq(e) => write!(f, "ZeroMQ error: {}", e),
            ChannelPairError::ChannelDisconnected(msg) => {
                write!(f, "channel disconnected: {}", msg)
            }
            #[allow(deprecated)]
            ChannelPairError::ChannelError(msg) => write!(f, "channel error: {}", msg),
            ChannelPairError::ConfigurationError(msg) => {
                write!(f, "configuration error: {}", msg)
            }
            ChannelPairError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ChannelPairError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ChannelPairError::Zmq(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ChannelPairError::ChannelDisconnected("test".into());
        assert!(err.to_string().contains("disconnected"));

        let err = ChannelPairError::Other("custom error".into());
        assert_eq!(err.to_string(), "custom error");
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ChannelPairError>();
    }
}
