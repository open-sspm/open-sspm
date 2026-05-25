(() => {
  const root = document.documentElement;

  const applyTheme = (dark) => {
    root.classList.toggle("dark", dark);
    try {
      localStorage.setItem("themeMode", dark ? "dark" : "light");
    } catch (_) {}
  };

  try {
    const stored = localStorage.getItem("themeMode");
    const prefersDark =
      typeof matchMedia === "function" &&
      matchMedia("(prefers-color-scheme: dark)").matches;
    if (stored ? stored === "dark" : prefersDark) {
      root.classList.add("dark");
    }
  } catch (_) {}

  try {
    const sidebarPref = localStorage.getItem("openSspm.sidebar.desktopOpen");
    if (sidebarPref === "false") root.dataset.sidebarPref = "closed";
    else if (sidebarPref === "true") root.dataset.sidebarPref = "open";
  } catch (_) {}

  document.addEventListener("osspm:theme", (event) => {
    const mode = event.detail?.mode;
    applyTheme(
      mode === "dark"
        ? true
        : mode === "light"
          ? false
          : !root.classList.contains("dark"),
    );
  });

  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!event.target.closest("[data-osspm-theme-toggle]")) return;
    document.dispatchEvent(new CustomEvent("osspm:theme"));
  });
})();
