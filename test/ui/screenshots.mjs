// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2026 martinx
// SPDX-License-Identifier: MPL-2.0
//
// Screenshots of the Access tab for the README and the landing page, from the
// demo cluster (demo.sh). Fails on console errors.
//   node screenshots.mjs <base url> <user> <password> <out dir>
import { chromium } from 'playwright'

const [base, user, pw, out] = process.argv.slice(2)
const shots = [
  ['overview', '#/access', 'Access', null],
  ['accounts', '#/access/users', 'Accounts', null],
  ['account', '#/access/users/orders-service', 'orders-service', null],
  ['authentication', '#/access/auth', 'Authentication', null],
  ['sessions', '#/access/sessions', 'Sessions', null],
]
const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1360, height: 860 }, deviceScaleFactor: 2 })
const errors = []
page.on('pageerror', e => errors.push(String(e) + ' @ ' + page.url() + ' ' + (e.stack || '').split('\n').slice(0, 3).join(' | ')))
page.on('console', m => { if (m.type() === 'error') errors.push(m.text()) })
await page.goto(base)
await page.fill('input[name=username]', user)
await page.fill('input[name=password]', pw)
await page.click('input[type=submit], button[type=submit]')
await page.waitForSelector('#main')
// let the management UI finish its first refresh: leaving the page while its
// first requests are in flight makes it report a connection error of its own
await page.waitForFunction(() => /Refreshed/.test(document.body.innerText), null, { timeout: 15000 })
for (const [name, hash, heading] of shots) {
  await page.goto(base + hash)
  await page.waitForFunction(h => document.querySelector('#main h1')?.textContent.includes(h), heading, { timeout: 15000 })
  await page.waitForTimeout(800)
  await page.screenshot({ path: `${out}/${name}.png` })
  console.log('shot', name)
}
await browser.close()
if (errors.length) { console.error(errors.join('\n')); process.exit(1) }
