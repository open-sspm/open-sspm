import {defineConfig} from "vitepress";

const repoName = process.env.GITHUB_REPOSITORY?.split("/")[1];
const ciBase = repoName ? `/${repoName}/` : "/";

export default defineConfig({
  title: "Open-SSPM",
  description: "Documentation for deploying, configuring, and operating Open-SSPM.",
  lang: "en-US",
  lastUpdated: true,
  cleanUrls: true,
  base: process.env.DOCS_BASE ?? (process.env.GITHUB_ACTIONS === "true" ? ciBase : "/"),
  themeConfig: {
    logo: "/logo.svg",
    nav: [
      {text: "Guide", link: "/getting-started"},
      {text: "Deployment", link: "/deployment"},
      {text: "Operations", link: "/operations"},
    ],
    sidebar: [
      {
        text: "Guide",
        items: [
          {text: "Overview", link: "/"},
          {text: "Getting Started", link: "/getting-started"},
          {text: "Architecture", link: "/architecture"},
          {text: "Connectors", link: "/connectors"},
          {text: "Operations", link: "/operations"},
        ],
      },
    ],
    search: {
      provider: "local",
    },
    socialLinks: [
      {icon: "github", link: "https://github.com/open-sspm/open-sspm"},
    ],
    footer: {
      message: "Open-SSPM documentation skeleton",
      copyright: "Copyright Open-SSPM contributors",
    },
  },
});
