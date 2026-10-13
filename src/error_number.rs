//! Every user-visible error ends with "(error N)."; its server response logs `error N Name`.
//! docs/errors.md lists the numbers.

/// The text a user sees: the sentence and its number.
pub fn numbered(sentence: &str, number: u16) -> String {
    format!("{sentence} (error {number}).")
}

/// The journal line of an error response: number and name, never addresses or secrets.
pub fn journal(number: u16, name: &str) -> String {
    format!("error {number} {name}")
}

pub fn log(number: u16, name: &str) {
    eprintln!("{}", journal(number, name));
}
