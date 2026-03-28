#!/usr/bin/env node
/**
 * SEC Parser Elite CLI
 * Node.js wrapper for the Python pipeline
 */

import { Command } from 'commander';
import chalk from 'chalk';
import ora from 'ora';
import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { table } from 'table';
import cliProgress from 'cli-progress';
import { logger } from './utils/logger';

const program = new Command();

// Path to Python executable and main.py
const PYTHON_PATH = process.env.PYTHON_PATH || 'python';
const PYTHON_MAIN = path.resolve(__dirname, '../../python/main.py');

interface RunResult {
  success: boolean;
  output: string;
  error?: string;
}

/**
 * Run a Python command and capture output
 */
async function runPythonCommand(args: string[]): Promise<RunResult> {
  return new Promise((resolve) => {
    const proc = spawn(PYTHON_PATH, [PYTHON_MAIN, ...args], {
      env: { ...process.env },
      stdio: ['inherit', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    proc.stdout?.on('data', (data) => {
      stdout += data.toString();
      process.stdout.write(data);
    });

    proc.stderr?.on('data', (data) => {
      stderr += data.toString();
      process.stderr.write(data);
    });

    proc.on('close', (code) => {
      resolve({
        success: code === 0,
        output: stdout,
        error: stderr || undefined,
      });
    });

    proc.on('error', (err) => {
      resolve({
        success: false,
        output: '',
        error: err.message,
      });
    });
  });
}

program
  .name('sec-parser')
  .description('SEC Parser Elite - N-CSR/N-CSRS Filing Parser CLI')
  .version('1.0.0');

/**
 * Parse a single filing by URL
 */
program
  .command('filing')
  .description('Parse a single filing by URL')
  .requiredOption('--url <url>', 'SEC filing URL')
  .action(async (options) => {
    const spinner = ora('Parsing filing...').start();

    try {
      const result = await runPythonCommand(['filing', '--url', options.url]);

      if (result.success) {
        spinner.succeed(chalk.green('Filing parsed successfully'));
      } else {
        spinner.fail(chalk.red('Filing parse failed'));
        if (result.error) {
          console.error(chalk.red(result.error));
        }
        process.exit(1);
      }
    } catch (err) {
      spinner.fail(chalk.red('Error running parser'));
      logger.error({ error: err }, 'Parser error');
      process.exit(1);
    }
  });

/**
 * Parse all filings for a CIK
 */
program
  .command('cik')
  .description('Parse all N-CSR filings for a company')
  .requiredOption('--cik <cik>', 'Company CIK number')
  .option('--limit <limit>', 'Maximum filings to process', '10')
  .action(async (options) => {
    console.log(chalk.blue(`\nFetching filings for CIK ${options.cik}...`));

    const progressBar = new cliProgress.SingleBar({
      format: 'Progress |' + chalk.cyan('{bar}') + '| {percentage}% | {value}/{total} filings',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
    });

    try {
      const result = await runPythonCommand([
        'cik',
        '--cik', options.cik,
        '--limit', options.limit,
      ]);

      if (!result.success) {
        console.error(chalk.red('\nProcessing failed'));
        process.exit(1);
      }
    } catch (err) {
      logger.error({ error: err }, 'Parser error');
      process.exit(1);
    }
  });

/**
 * Parse multiple filings from a file
 */
program
  .command('batch')
  .description('Parse multiple filings from a file')
  .requiredOption('--file <path>', 'File with URLs (one per line)')
  .option('--concurrency <n>', 'Number of concurrent processes', '3')
  .action(async (options) => {
    // Validate file exists
    if (!fs.existsSync(options.file)) {
      console.error(chalk.red(`File not found: ${options.file}`));
      process.exit(1);
    }

    // Count URLs
    const content = fs.readFileSync(options.file, 'utf-8');
    const urls = content.split('\n').filter((line) => line.trim().startsWith('http'));

    console.log(chalk.blue(`\nProcessing ${urls.length} filings with concurrency ${options.concurrency}...`));

    const progressBar = new cliProgress.SingleBar({
      format: 'Progress |' + chalk.cyan('{bar}') + '| {percentage}% | {value}/{total}',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
    });

    try {
      const result = await runPythonCommand([
        'batch',
        '--file', options.file,
        '--concurrency', options.concurrency,
      ]);

      if (!result.success) {
        console.error(chalk.red('\nBatch processing failed'));
        process.exit(1);
      }
    } catch (err) {
      logger.error({ error: err }, 'Parser error');
      process.exit(1);
    }
  });

/**
 * Retry a failed filing
 */
program
  .command('retry')
  .description('Retry a failed filing by ID')
  .requiredOption('--id <id>', 'Filing UUID to retry')
  .action(async (options) => {
    const spinner = ora('Retrying filing...').start();

    try {
      const result = await runPythonCommand(['retry', '--id', options.id]);

      if (result.success) {
        spinner.succeed(chalk.green('Filing retried successfully'));
      } else {
        spinner.fail(chalk.red('Retry failed'));
        process.exit(1);
      }
    } catch (err) {
      spinner.fail(chalk.red('Error'));
      logger.error({ error: err }, 'Retry error');
      process.exit(1);
    }
  });

/**
 * Show status of all filings
 */
program
  .command('status')
  .description('Show status of all filings')
  .action(async () => {
    try {
      await runPythonCommand(['status']);
    } catch (err) {
      logger.error({ error: err }, 'Status error');
      process.exit(1);
    }
  });

// Parse arguments
program.parse(process.argv);

// Show help if no command
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
