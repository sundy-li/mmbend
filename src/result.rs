use databend_driver::Error;

pub type Result<T, E = Error> = core::result::Result<T, E>;
