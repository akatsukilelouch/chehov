use std::path::Path;

use ::tokio::io::{AsyncRead, AsyncWrite};

pub trait View {
    type Reader<'this>: Unpin + AsyncRead + 'this
    where
        Self: 'this;

    type Error;

    fn open(
        &self,
        path: impl AsRef<Path>,
    ) -> impl IntoFuture<Output = Result<Self::Reader<'_>, Self::Error>>;
}

pub trait Append {
    type Writer<'this>: Unpin + AsyncWrite + 'this
    where
        Self: 'this;

    type Error;

    fn open(
        &self,
        path: impl AsRef<Path>,
    ) -> impl IntoFuture<Output = Result<Self::Writer<'_>, Self::Error>>;
}

#[cfg(feature = "fs")]
pub mod tokio {
    use crate::fs::Append;

    use super::View;
    use std::{io::Error, path::Path};
    use tokio::fs::{File, OpenOptions};

    pub struct Tokio;

    impl View for Tokio {
        type Reader<'this> = File;

        type Error = Error;

        #[inline]
        fn open(
            &self,
            path: impl AsRef<Path>,
        ) -> impl IntoFuture<Output = Result<Self::Reader<'_>, Self::Error>> {
            File::open(path)
        }
    }

    impl Append for Tokio {
        type Writer<'this> = File;

        type Error = Error;

        #[inline]
        fn open(
            &self,
            path: impl AsRef<Path>,
        ) -> impl IntoFuture<Output = Result<Self::Writer<'_>, Self::Error>> {
            async move { OpenOptions::new().create(true).write(true).open(path).await }
        }
    }
}
