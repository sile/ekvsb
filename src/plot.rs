use plotlib::page::Page;
use plotlib::scatter::{Scatter, Style};
use plotlib::style::Point;
use plotlib::view::View;
use rand::{self, Rng};
use std::path::Path;
use trackable::error::Failed;

use task::TaskResult;
use Result;

#[derive(Debug)]
pub struct PlotOptions {
    sampling_rate: f64,
    y_max: Option<f64>,
}
impl PlotOptions {
    pub fn new() -> PlotOptions {
        PlotOptions {
            sampling_rate: 1.0,
            y_max: None,
        }
    }

    pub fn sampling_rate(&mut self, sampling_rate: f64) -> &mut Self {
        self.sampling_rate = sampling_rate;
        self
    }

    pub fn y_max(&mut self, y_max: f64) -> &mut Self {
        self.y_max = Some(y_max);
        self
    }

    pub fn plot_text(&self, results: &[TaskResult]) -> Result<String> {
        track!(self.with_view(results, |v| Page::single(&v).to_text()))
    }

    pub fn plot_svg<P: AsRef<Path>>(&self, results: &[TaskResult], svg_file: P) -> Result<()> {
        track!(self.with_view(results, |v| Page::single(&v).save(svg_file)))
    }

    fn with_view<F, T>(&self, results: &[TaskResult], f: F) -> Result<T>
    where
        F: FnOnce(View) -> T,
    {
        track_assert!(self.sampling_rate > 0.0, Failed; self.sampling_rate);
        track_assert!(self.sampling_rate <= 1.0, Failed; self.sampling_rate);
        let times = results.iter().map(|r| r.elapsed).collect::<Vec<_>>();
        let data = times
            .iter()
            .cloned()
            .enumerate()
            .map(|(x, y)| (x as f64, y.as_f64()))
            .filter(|_| rand::thread_rng().gen_range(0.0, 1.0) < self.sampling_rate)
            .filter(|t| self.y_max.is_none() || Some(t.1) <= self.y_max)
            .collect::<Vec<_>>();
        let s = Scatter::from_vec(&data).style(&Style::new().size(1.0));
        let y_max = self
            .y_max
            .unwrap_or_else(|| times.iter().max().map(|x| x.as_f64()).unwrap_or(0.0));
        let v = View::new()
            .add(&s)
            .x_range(0.0, times.len() as f64)
            .y_range(0.0, y_max)
            .x_label("Sequence Number")
            .y_label("Latency Seconds");
        Ok(f(v))
    }
}
