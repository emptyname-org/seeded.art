// Long descriptions expand without losing the way back to the artwork.
for (const text of document.querySelectorAll("[data-read-more]")) {
  const line = parseFloat(getComputedStyle(text).lineHeight) || 24;
  if (text.scrollHeight <= line * 5 + 1) continue;
  text.style.maxHeight = `${line * 3}px`;
  text.classList.add("read-more-collapsed");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "read-more-button";
  button.textContent = "Read more";
  button.setAttribute("aria-expanded", "false");
  button.addEventListener("click", () => {
    const expanded = button.getAttribute("aria-expanded") === "true";
    text.style.maxHeight = expanded ? `${line * 3}px` : "";
    text.classList.toggle("read-more-collapsed", expanded);
    button.setAttribute("aria-expanded", String(!expanded));
    button.textContent = expanded ? "Read more" : "Read less";
    if (expanded) text.scrollIntoView({ block: "nearest" });
  });
  text.after(button);
}
