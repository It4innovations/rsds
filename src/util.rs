/// Use with caution!
pub trait ResultExt<T> {
    fn ensure(self) -> T;
}

impl<T, E: std::fmt::Debug> ResultExt<T> for Result<T, E> {
    fn ensure(self) -> T {
        //        self.unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() })
        self.unwrap()
    }
}

pub trait OptionExt<T> {
    fn ensure(self) -> T;
}

impl<T> OptionExt<T> for Option<T> {
    fn ensure(self) -> T {
        //        self.unwrap_or_else(|| unsafe { std::hint::unreachable_unchecked() })
        self.unwrap()
    }
}
