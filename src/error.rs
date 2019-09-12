quick_error! {
    #[derive(Debug)]
    pub enum DsError {
        IOError(err: std::io::Error) {
            cause(err)
            description(err.description())
            from()
        }
        SerializationError(err: Box<std::error::Error>) {
            cause(&**err)
            description(err.description())
            from(err: serde_json::error::Error) -> (Box::new(err))
            from(err: rmp_serde::encode::Error) -> (Box::new(err))
        }
    }
}
