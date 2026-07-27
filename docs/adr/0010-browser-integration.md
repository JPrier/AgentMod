# ADR 0010: Browser integration

Status: Accepted

Rendered browsing runs in a managed browser host using a configured W3C WebDriver
endpoint. The dependency layer owns protocol details while AgentMod owns lifecycle,
health, permissions, downloads, artifacts, and final-URL revalidation. Loopback
WebDriver HTTP is allowed; remote control endpoints require TLS.

A configuration entry alone is not browser support. Release evidence must exercise
navigation, inspection, screenshots, interaction, forms, downloads, rendered pages,
and shutdown through the process boundary. `runtime_browser_loop` is that evidence.
