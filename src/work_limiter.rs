use std::time::{Duration, Instant};

/// Limits the amount of work performed in one driver cycle.
#[derive(Debug)]
pub(crate) struct WorkLimiter {
    mode: Mode,
    cycle: u16,
    start_time: Option<Instant>,
    completed: usize,
    allowed: usize,
    desired_cycle_time: Duration,
    smoothed_time_per_work_item_nanos: f64,
}

impl WorkLimiter {
    pub(crate) fn new(desired_cycle_time: Duration) -> Self {
        Self {
            mode: Mode::Measure,
            cycle: 0,
            start_time: None,
            completed: 0,
            allowed: 0,
            desired_cycle_time,
            smoothed_time_per_work_item_nanos: 0.0,
        }
    }

    pub(crate) fn start_cycle(&mut self, now: impl Fn() -> Instant) {
        self.completed = 0;
        if let Mode::Measure = self.mode {
            self.start_time = Some(now());
        }
    }

    pub(crate) fn allow_work(&mut self, now: impl Fn() -> Instant) -> bool {
        match self.mode {
            Mode::Measure => (now() - self.start_time.unwrap()) < self.desired_cycle_time,
            Mode::HistoricData => self.completed < self.allowed,
        }
    }

    pub(crate) fn record_work(&mut self, work: usize) {
        self.completed += work;
    }

    pub(crate) fn finish_cycle(&mut self, now: impl Fn() -> Instant) {
        if self.completed == 0 {
            return;
        }

        if let Mode::Measure = self.mode {
            let elapsed = now() - self.start_time.unwrap();
            let time_per_work_item_nanos = elapsed.as_nanos() as f64 / self.completed as f64;
            self.smoothed_time_per_work_item_nanos = if self.allowed == 0 {
                time_per_work_item_nanos
            } else {
                (7.0 * self.smoothed_time_per_work_item_nanos + time_per_work_item_nanos) / 8.0
            }
            .max(1.0);
            self.allowed = (self.desired_cycle_time.as_nanos() as f64
                / self.smoothed_time_per_work_item_nanos) as usize;
            self.allowed = self.allowed.max(1);
            self.start_time = None;
        }

        self.cycle = self.cycle.wrapping_add(1);
        self.mode = match self.cycle % SAMPLING_INTERVAL {
            0 => Mode::Measure,
            _ => Mode::HistoricData,
        };
    }
}

const SAMPLING_INTERVAL: u16 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Measure,
    HistoricData,
}
