import {defineConfig} from "vitepress";

const repoName = process.env.GITHUB_REPOSITORY?.split("/")[1];
const ciBase = repoName ? `/${repoName}/` : "/";
const docsBase = process.env.DOCS_BASE?.trim();
const base = docsBase == null
  ? (process.env.GITHUB_ACTIONS === "true" ? ciBase : "/")
  : (docsBase === "" || docsBase === "/" ? "/" : (docsBase.endsWith("/") ? docsBase : `${docsBase}/`));

export default defineConfig({
  title: "Open-SSPM",
  description: "Documentation for deploying, configuring, and operating Open-SSPM - the open-source IAM and SaaS governance platform.",
  lang: "en-US",
  lastUpdated: true,
  cleanUrls: true,
  head: [
    ["link", {rel: "icon", href: "/shield.png", type: "image/png"}],
  ],
  base,
  themeConfig: {
    siteTitle: false,
    logo: {
      light: "/logo.svg",
      dark: "/logo-dark.svg",
    },
    nav: [
      {text: "Installation", link: "/install/"},
      {text: "Configuration", link: "/config/"},
      {text: "Connectors", link: "/config/connectors/"},
      {text: "Running", link: "/run/"},
      {text: "GitHub", link: "https://github.com/open-sspm/open-sspm"},
    ],
    sidebar: {
      "/": [
        {
          text: "Getting Started",
          items: [
            {text: "Introduction", link: "/"},
            {text: "Quick Start", link: "/#quick-start"},
            {text: "Live Demo", link: "/#live-demo"},
          ],
        },
      ],
      "/install/": [
        {
          text: "Installation",
          items: [
            {text: "Overview", link: "/install/"},
            {text: "Docker Compose", link: "/install/docker"},
            {text: "Kubernetes (Helm)", link: "/install/kubernetes"},
          ],
        },
        {
          text: "Prerequisites",
          items: [
            {text: "Database Setup", link: "/config/database"},
            {text: "Connector Secret Key", link: "/install/#generate-a-connector-secret-key"},
          ],
        },
      ],
      "/config/": [
        {
          text: "Configuration",
          items: [
            {text: "Overview", link: "/config/"},
            {text: "Environment Variables", link: "/config/environment-variables"},
            {text: "Database", link: "/config/database"},
            {text: "Authentication", link: "/config/authentication"},
          ],
        },
        {
          text: "Connectors",
          items: [
            {text: "Connectors Overview", link: "/config/connectors/"},
            {text: "Okta", link: "/config/connectors/okta"},
            {text: "Microsoft Entra ID", link: "/config/connectors/entra"},
            {text: "Google Workspace", link: "/config/connectors/google-workspace"},
            {text: "GitHub", link: "/config/connectors/github"},
            {text: "Datadog", link: "/config/connectors/datadog"},
            {text: "AWS Identity Center", link: "/config/connectors/aws"},
          ],
        },
      ],
      "/config/connectors/": [
        {
          text: "Connectors",
          items: [
            {text: "Overview", link: "/config/connectors/"},
          ],
        },
        {
          text: "Identity Providers",
          items: [
            {text: "Okta", link: "/config/connectors/okta"},
            {text: "Microsoft Entra ID", link: "/config/connectors/entra"},
          ],
        },
        {
          text: "Connected Apps",
          items: [
            {text: "Google Workspace", link: "/config/connectors/google-workspace"},
            {text: "GitHub", link: "/config/connectors/github"},
            {text: "Datadog", link: "/config/connectors/datadog"},
            {text: "AWS Identity Center", link: "/config/connectors/aws"},
          ],
        },
      ],
      "/run/": [
        {
          text: "Running & Operations",
          items: [
            {text: "Overview", link: "/run/"},
            {text: "Starting Components", link: "/run/#starting-the-application"},
            {text: "Viewing Logs", link: "/run/#viewing-logs"},
            {text: "Manual Sync", link: "/run/#manual-sync-operations"},
            {text: "Health Checks", link: "/run/#health-checks"},
            {text: "Backup", link: "/run/#backup-operations"},
            {text: "Updates", link: "/run/#updates-and-upgrades"},
            {text: "Troubleshooting", link: "/run/#troubleshooting"},
          ],
        },
      ],
    },
    search: {
      provider: "local",
    },
    socialLinks: [
      {icon: "github", link: "https://github.com/open-sspm/open-sspm"},
    ],
    footer: {
      message: "Released under the MIT License.",
      copyright: "Copyright Open-SSPM contributors",
    },
  },
});
