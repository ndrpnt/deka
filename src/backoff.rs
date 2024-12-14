use ::backoff as backoffcrate;
use std::time::Duration;

/// Provides a local backoff trait that mirrors the external [`Backoff`][1] one.
/// All [`Backoff`][1] implementations can be used interchangeably with this trait.
///
/// [1]: backoffcrate::backoff::Backoff
pub trait Backoff {
    fn reset(&mut self) {}
    fn next_backoff(&mut self) -> Option<Duration>;
}

impl<T> Backoff for T
where
    T: backoffcrate::backoff::Backoff,
{
    fn next_backoff(&mut self) -> Option<Duration> {
        self.next_backoff()
    }

    fn reset(&mut self) {
        self.reset()
    }
}

#[derive(Clone)]
pub(crate) struct BackoffWrapper<T: Backoff>(pub T);

impl<T> backoffcrate::backoff::Backoff for BackoffWrapper<T>
where
    T: Backoff,
{
    fn next_backoff(&mut self) -> Option<Duration> {
        self.0.next_backoff()
    }

    fn reset(&mut self) {
        self.0.reset()
    }
}
