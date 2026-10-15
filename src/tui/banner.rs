//! The block-letter banner: `MENTIS` and `CHAT` in the same font as the
//! MentisDB console.

pub(super) const TITLE: &str = "MENTIS CHAT";

/// Rows in the block-letter banner.
pub(super) const BANNER_ROWS: usize = 6;

/// `MENTIS` in the same block letters as the MentisDB banner.
pub(super) const MENTIS_PART: [&str; BANNER_ROWS] = [
    "███╗   ███╗███████╗███╗   ██╗████████╗██╗███████╗",
    "████╗ ████║██╔════╝████╗  ██║╚══██╔══╝██║██╔════╝",
    "██╔████╔██║█████╗  ██╔██╗ ██║   ██║   ██║███████╗",
    "██║╚██╔╝██║██╔══╝  ██║╚██╗██║   ██║   ██║╚════██║",
    "██║ ╚═╝ ██║███████╗██║ ╚████║   ██║   ██║███████║",
    "╚═╝     ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝╚══════╝",
];

/// `CHAT` in the same font, led by the gap between the two words.
pub(super) const CHAT_PART: [&str; BANNER_ROWS] = [
    "    ██████╗██╗  ██╗ █████╗ ████████╗",
    "   ██╔════╝██║  ██║██╔══██╗╚══██╔══╝",
    "   ██║     ███████║███████║   ██║   ",
    "   ██║     ██╔══██║██╔══██║   ██║   ",
    "   ╚██████╗██║  ██║██║  ██║   ██║   ",
    "    ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ",
];

pub(super) fn banner_width() -> u16 {
    (MENTIS_PART[0].chars().count() + CHAT_PART[0].chars().count()) as u16
}
