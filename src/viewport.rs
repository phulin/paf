use ratatui::text::Line;

#[derive(Clone, Debug, Default)]
pub(crate) struct TimelineRenderCache {
    pub run_id: String,
    pub sequence: u64,
    pub recent_len: usize,
    pub status: Option<String>,
    pub width: u16,
    pub lines: Vec<Line<'static>>,
    pub offsets: Vec<usize>,
    pub rendered_lines: usize,
}
