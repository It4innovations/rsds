quick_error! {
    #[derive(Debug)]
    pub enum DsError {
        IOError(err: std::io::Error) {
            cause(err)
            description(err.description())
            from()
        }
    }
}
