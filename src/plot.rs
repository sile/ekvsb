use crate::task::TaskResult;
use crate::Result;
use gnuplot::{AutoOption, AxesCommon, Figure, Rotate};
use rand::{self, Rng};
use std::f64::EPSILON;
use trackable::error::Failed;

#[derive(Debug)]
pub struct PlotOptions {
    pub title: String,
    pub output_file: String,
    pub terminal: String,
    pub sampling_rate: f64,
    pub logscale: bool,
    pub y_max: Option<f64>,
}
impl PlotOptions {
    pub fn new() -> PlotOptions {
        PlotOptions {
            title: String::new(),
            output_file: String::new(),
            terminal: "dumb".to_owned(),
            sampling_rate: 1.0,
            logscale: false,
            y_max: None,
        }
    }

    pub fn plot(&self, results: &[TaskResult]) -> Result<()> {
        track_assert!(self.sampling_rate > 0.0, Failed; self.sampling_rate);
        track_assert!(self.sampling_rate <= 1.0, Failed; self.sampling_rate);

        let times = results.iter().map(|r| r.elapsed).collect::<Vec<_>>();
        let data = times
            .iter()
            .cloned()
            .enumerate()
            .map(|(x, y)| (x as f64, y.as_f64()))
            .filter(|_| rand::thread_rng().gen_range(0.0, 1.0) < self.sampling_rate)
            .collect::<Vec<_>>();
        let xs = data.iter().map(|t| t.0);
        let ys = data.iter().map(|t| t.1);

        let x_label = if (self.sampling_rate - 1.0).abs() < EPSILON {
            "Sequence Number".to_owned()
        } else {
            format!("Sequence Number (sampling-rate={})", self.sampling_rate)
        };
        let mut y_label = "Latency Seconds".to_owned();
        if let Some(y_max) = self.y_max {
            let out_of_ranges = data.iter().filter(|t| t.1 > y_max).count();
            if out_of_ranges > 0 {
                y_label = format!("Latency Seconds (overflowed-samples={})", out_of_ranges);
            }
        };

        let mut fg = Figure::new();
        let axes = fg.axes2d();
        axes.set_title(&self.title, &[])
            .points(xs, ys, &[])
            .set_x_label(&x_label, &[])
            .set_x_ticks(Some((AutoOption::Auto, 0)), &[], &[Rotate(270.0)])
            .set_x_range(AutoOption::Fix(0.0), AutoOption::Fix(results.len() as f64))
            .set_y_label(&y_label, &[])
            .set_y_range(
                AutoOption::Auto,
                self.y_max.map_or(AutoOption::Auto, AutoOption::Fix),
            );
        if self.logscale {
            axes.set_y_log(Some(10.0));
        }
        fg.set_terminal(&self.terminal, &self.output_file);
        fg.show();
        Ok(())
    }
}
impl Default for PlotOptions {
    fn default() -> Self {
        Self::new()
    }
}
