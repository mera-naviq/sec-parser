/**
 * CLI Progress Bar Utilities
 */

import cliProgress from 'cli-progress';
import chalk from 'chalk';

export function createProgressBar(total: number, description: string = 'Progress') {
  const bar = new cliProgress.SingleBar({
    format: `${description} |${chalk.cyan('{bar}')}| {percentage}% | {value}/{total} | ETA: {eta}s`,
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true,
  });

  bar.start(total, 0);
  return bar;
}

export function createMultiBar() {
  return new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: '{name} |{bar}| {percentage}% | {value}/{total}',
  }, cliProgress.Presets.shades_classic);
}
