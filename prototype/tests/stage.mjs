// Test helpers for the browser host's devices: stage ui/runtime and plugins/ in
// the layout the built site uses (runtime/… next to plugins/…), so the page-side
// modules resolve their imports exactly as they do in dist/.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, '..');

export const FAKE_CHEERPX = pathToFileURL(path.join(here, 'fake-cheerpx.mjs')).href;

let staged = null;
export function stageSite() {
  if (staged) return staged;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-site-'));
  fs.cpSync(path.join(root, 'ui', 'runtime'), path.join(dir, 'runtime'), { recursive: true });
  fs.cpSync(path.join(root, 'plugins'), path.join(dir, 'plugins'), { recursive: true, filter: (p) => !p.includes('node_modules') });
  staged = dir;
  return dir;
}

/** The page-side device registry, as the browser host constructs it. */
export async function hostDevices() {
  const { Devices } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'devices.js')).href);
  return new Devices();
}

/** A plugin `host` API whose `device` calls cross a JSON boundary, like the wire. */
export function jsonHost(devices, plugin = 'linux-sandbox', manifest = { devices: ['linux-vm'] }) {
  const wire = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
  return {
    device: async (device, op, args = {}, config = {}) => wire(await devices.call(plugin, manifest, wire({ device, op, args, config }))),
  };
}

/** VM config pointing the device at the fake CheerpX and temp mount paths. */
export function fakeVmConfig() {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'sbx-'));
  return {
    cheerpx_url: FAKE_CHEERPX,
    in_path: path.join(base, 'in'),
    out_path: path.join(base, 'out'),
    workspace_path: path.join(base, 'workspace'),
    workspace: `test-${path.basename(base)}`,
  };
}
