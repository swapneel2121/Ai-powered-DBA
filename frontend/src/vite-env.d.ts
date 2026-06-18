/// <reference types="vite/client" />

// Tells TypeScript to accept CSS file imports (e.g. import './index.css')
declare module "*.css" {
  const content: Record<string, string>;
  export default content;
}
