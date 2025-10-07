use crate::blame::Asset360ChangeMeta;

pub fn path_to_string(segments: &[String]) -> String {
    if segments.is_empty() {
        return "<root>".into();
    }
    let mut out = String::new();
    for segment in segments {
        if segment.chars().all(|c| c.is_ascii_digit()) {
            out.push_str(&format!("[{segment}]"));
        } else {
            if !out.is_empty() {
                out.push('.');
            }
            out.push_str(segment);
        }
    }
    out
}

pub fn format_stage_entries(entries: &[(Vec<String>, Asset360ChangeMeta)]) -> String {
    if entries.is_empty() {
        return "<empty stage map>".to_string();
    }
    let mut lines = Vec::with_capacity(entries.len());
    for (path, meta) in entries {
        lines.push(format!(
            "{} => change_id={} author={} timestamp={} source={} ics_id={}",
            path_to_string(path),
            meta.change_id,
            meta.author,
            meta.timestamp,
            meta.source,
            meta.ics_id
        ));
    }
    lines.join("\n")
}
