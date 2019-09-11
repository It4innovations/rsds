quick_error! {
    #[derive(Debug)]
    pub enum DsError {
        IOError(err: std::io::Error) {
            cause(err)
            description(err.description())
            from()
        }
        SerializationError(err: serde_json::Error) {
            cause(err)
            description(err.description())
            from()
        }
    }
}
