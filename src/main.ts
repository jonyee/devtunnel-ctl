#!/usr/bin/env node
import { fileURLToPath } from 'node:url';
import { cli, report } from './cli.ts';

await cli(process.argv.slice(2), fileURLToPath(import.meta.url)).catch(report);
