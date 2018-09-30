use plotlib::page::Page;
use plotlib::scatter::{Scatter, Style};
use plotlib::style::Point;
use plotlib::view::View;
use rand::{self, Rng};
use std::path::Path;
use trackable::error::Failed;

use task::TaskResult;
use Result;

pub fn plot_text(results: &[TaskResult], sampling_rate: f64) -> Result<String> {
    track_assert!(sampling_rate > 0.0, Failed; sampling_rate);
    track_assert!(sampling_rate <= 1.0, Failed; sampling_rate);

    let times = results.iter().map(|r| r.elapsed).collect::<Vec<_>>();
    let data = times
        .iter()
        .cloned()
        .enumerate()
        .map(|(x, y)| (x as f64, y.as_f64()))
        .filter(|_| rand::thread_rng().gen_range(0.0, 1.0) < sampling_rate)
        .collect::<Vec<_>>();
    let s = Scatter::from_vec(&data).style(&Style::new());
    let v = View::new()
        .add(&s)
        .x_range(0.0, times.len() as f64)
        .y_range(0.0, times.iter().max().map(|x| x.as_f64()).unwrap_or(0.0))
        .x_label("Sequence Number")
        .y_label("Latency Seconds");
    Ok(Page::single(&v).to_text())
}

pub fn plot_svg<P: AsRef<Path>>(
    results: &[TaskResult],
    svg_file: P,
    sampling_rate: f64,
) -> Result<()> {
    track_assert!(sampling_rate > 0.0, Failed; sampling_rate);
    track_assert!(sampling_rate <= 1.0, Failed; sampling_rate);

    let times = results.iter().map(|r| r.elapsed).collect::<Vec<_>>();
    let data = times
        .iter()
        .cloned()
        .enumerate()
        .map(|(x, y)| (x as f64, y.as_f64()))
        .filter(|_| rand::thread_rng().gen_range(0.0, 1.0) < sampling_rate)
        .collect::<Vec<_>>();
    let s = Scatter::from_vec(&data).style(&Style::new().size(1.0));
    let v = View::new()
        .add(&s)
        .x_range(0.0, times.len() as f64)
        .y_range(0.0, times.iter().max().map(|x| x.as_f64()).unwrap_or(0.0))
        .x_label("Sequence Number")
        .y_label("Latency Seconds");
    Page::single(&v).save(svg_file);
    Ok(())
}
