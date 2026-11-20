// LessorMQ admin, the example: a small management interface on the public v1 API, in
// one file without a build step. It proves the API is enough to run an installation:
// tenants, their keys, queues, the dead-letter set.
//
// It talks to the server straight from the browser, so the server must allow this page's
// origin (CORS_ALLOW_ORIGIN; see docs/self-hosting.md). The key you sign in with stays in
// this tab's sessionStorage and goes only to that server.
//
// - An operator key manages every tenant; a tenant is then opened with the
//   LessorMQ-Tenant header, as an admin key of that tenant.
// - A tenant's admin key manages just that tenant.
//
// Everything the server sends is put on the page as text, never as HTML: message bodies
// and error messages come from workers.

const STORE = 'lessormq-admin-example'
let session = load()

// ---------------------------------------------------------------------------- the API

class ApiError extends Error {
  constructor(status, body) {
    super((body && (body.detail || body.title)) || `HTTP ${status}`)
    this.status = status
    this.code = body && body.code
    this.errors = (body && body.errors) || []
  }
}

async function call(method, path, { tenant, body, query } = {}) {
  const url = new URL(session.url.replace(/\/+$/, '') + path)
  for (const [name, value] of Object.entries(query || {})) {
    if (value !== undefined && value !== '') url.searchParams.set(name, value)
  }
  const headers = { Authorization: `Bearer ${session.key}`, Accept: 'application/json' }
  if (tenant && session.mode === 'operator') headers['LessorMQ-Tenant'] = tenant
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  let response
  try {
    response = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) })
  } catch {
    throw new ApiError(0, { detail: `Cannot reach ${session.url}. Is it running, and does it allow this origin (CORS_ALLOW_ORIGIN)?` })
  }
  const text = await response.text()
  let data = null
  try { data = text ? JSON.parse(text) : null } catch { data = { detail: text.slice(0, 200) } }
  if (!response.ok) throw new ApiError(response.status, data)
  return data
}

/** The paths for a tenant's keys differ: an operator names the tenant, its own key does not. */
const keysPath = (tenant) => (session.mode === 'operator' ? `/v1/tenants/${enc(tenant)}/api-keys` : '/v1/api-keys')
const enc = encodeURIComponent

// ---------------------------------------------------------------------------- the page

/** h('a', {href: '#/'}, 'text', child): an element; strings become text nodes. */
function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag)
  for (const [name, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue
    if (name.startsWith('on')) el.addEventListener(name.slice(2), value)
    else if (name === 'value') el.value = value
    else el.setAttribute(name, value === true ? '' : value)
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue
    el.append(child instanceof Node ? child : document.createTextNode(String(child)))
  }
  return el
}

const main = () => document.getElementById('main')
const show = (...nodes) => main().replaceChildren(...nodes)
const when = (iso) => (iso ? new Date(iso).toLocaleString() : '-')
const n = (value) => (value === null || value === undefined ? '-' : Number(value).toLocaleString())
const seconds = (s) => (s < 60 ? `${s}s` : s < 3600 ? `${Math.round(s / 60)}m` : s < 86400 ? `${(s / 3600).toFixed(1)}h` : `${(s / 86400).toFixed(1)}d`)

function message(text, kind = 'error') {
  const box = document.getElementById('message')
  if (box) box.replaceChildren(text ? h('p', { class: kind }, text) : '')
}

/** Runs an action and shows its error; returns whether it worked. */
async function attempt(action, done) {
  message('')
  try {
    await action()
    if (done) message(done, 'notice')
    return true
  } catch (error) {
    const fields = (error.errors || []).map((e) => `${e.field}: ${e.message}`).join('; ')
    message(fields ? `${error.message} (${fields})` : error.message)
    return false
  }
}

/** A button that asks once more before it acts. */
function confirmButton(label, action, { danger = false } = {}) {
  const button = h('button', { class: danger ? 'danger' : undefined, type: 'button' }, label)
  let armed = false
  button.addEventListener('click', async () => {
    if (!armed) {
      armed = true
      button.textContent = `${label}: sure?`
      setTimeout(() => { armed = false; button.textContent = label }, 4000)
      return
    }
    button.disabled = true
    await action()
    button.disabled = false
  })
  return button
}

function secretBox(text, secret) {
  return h('div', { class: 'secret' },
    h('p', {}, text, ' It is shown only now.'),
    h('code', {}, secret), ' ',
    h('button', { type: 'button', onclick: () => navigator.clipboard.writeText(secret) }, 'Copy'))
}

// ---------------------------------------------------------------------------- session

function load() {
  try { return JSON.parse(sessionStorage.getItem(STORE)) || null } catch { return null }
}

function save(value) {
  session = value
  try {
    if (value) sessionStorage.setItem(STORE, JSON.stringify(value))
    else sessionStorage.removeItem(STORE)
  } catch { /* private mode: the session lasts as long as the page */ }
}

function header() {
  const nav = document.getElementById('nav')
  const who = document.getElementById('who')
  if (!session) {
    nav.replaceChildren()
    who.replaceChildren()
    return
  }
  nav.replaceChildren(session.mode === 'operator'
    ? h('a', { href: '#/' }, 'Tenants')
    : h('a', { href: `#/tenant/${enc(session.tenant)}` }, session.tenant))
  who.replaceChildren(`${session.mode} on ${session.url} `,
    h('button', { type: 'button', onclick: () => { save(null); location.hash = '#/'; route() } }, 'Disconnect'))
}

function connectPage() {
  const url = h('input', { name: 'url', size: 32, value: 'http://127.0.0.1:18080', required: true })
  const key = h('input', { name: 'key', type: 'password', size: 40, placeholder: 'lmq_...', required: true, autocomplete: 'off' })
  show(
    h('h1', {}, 'Connect'),
    h('p', {}, 'An operator key manages every tenant; a tenant admin key manages that tenant.'),
    h('form', {
      onsubmit: async (event) => {
        event.preventDefault()
        save({ url: url.value.trim(), key: key.value.trim(), mode: 'operator' })
        const ok = await attempt(async () => {
          try {
            await call('GET', '/v1/tenants', { query: { limit: 1 } })
          } catch (error) {
            if (error.status !== 403) throw error
            const tenant = await call('GET', '/v1/tenant')
            save({ ...session, mode: 'tenant', tenant: tenant.id })
          }
        })
        if (!ok) { save(null); return }
        location.hash = session.mode === 'operator' ? '#/' : `#/tenant/${enc(session.tenant)}`
        route()
      },
    }, h('label', {}, 'Server', url), h('label', {}, 'Key', key), h('button', {}, 'Connect')),
    h('div', { id: 'message' }),
  )
}

// ---------------------------------------------------------------------------- tenants

async function tenantsPage() {
  const list = h('tbody')
  const name = h('input', { required: true, maxlength: 255 })
  const id = h('input', { pattern: '[a-z0-9][a-z0-9-]{1,62}', placeholder: 'from the name' })
  const created = h('div')

  show(
    h('h1', {}, 'Tenants'),
    h('form', {
      onsubmit: async (event) => {
        event.preventDefault()
        const body = { name: name.value.trim() }
        if (id.value.trim()) body.id = id.value.trim()
        await attempt(async () => {
          const tenant = await call('POST', '/v1/tenants', { body })
          created.replaceChildren(secretBox(`Tenant ${tenant.id} has an admin key:`, tenant.admin_key.secret))
          name.value = ''
          id.value = ''
          await fill()
        })
      },
    }, h('label', {}, 'Name', name), h('label', {}, 'Id', id), h('button', {}, 'Create tenant')),
    created,
    h('div', { id: 'message' }),
    h('table', {},
      h('thead', {}, h('tr', {}, h('th', {}, 'Tenant'), h('th', {}, 'Status'), h('th', { class: 'num' }, 'Queues'),
        h('th', { class: 'num' }, 'Open'), h('th', { class: 'num' }, 'Dead-letter'), h('th', {}, 'Created'))),
      list),
  )

  async function fill() {
    const rows = []
    let cursor
    do {
      const page = await call('GET', '/v1/tenants', { query: { limit: 100, cursor } })
      rows.push(...page.items)
      cursor = page.next_cursor || undefined
    } while (cursor)
    list.replaceChildren(...rows.filter((t) => t.status !== 'deleting').map((t) => h('tr', {},
      h('td', {}, h('a', { href: `#/tenant/${enc(t.id)}` }, t.name), h('div', { class: 'muted' }, h('code', {}, t.id))),
      h('td', {}, t.status),
      h('td', { class: 'num' }, n(t.usage.queues)),
      h('td', { class: 'num' }, n(t.usage.open_messages)),
      h('td', { class: 'num' }, n(t.usage.failed_messages)),
      h('td', {}, when(t.created_at)))))
    if (rows.length === 0) list.replaceChildren(h('tr', {}, h('td', { colspan: 6, class: 'muted' }, 'No tenants yet.')))
  }

  await attempt(fill)
}

// ---------------------------------------------------------------------------- one tenant

async function tenantPage(tenantId, tab) {
  const base = `#/tenant/${enc(tenantId)}`
  const tabs = [['queues', 'Queues'], ['keys', 'API keys'], ['settings', 'Settings']]
  const body = h('div')
  show(
    h('h1', {}, 'Tenant ', h('code', {}, tenantId)),
    h('div', { class: 'tabs' }, tabs.map(([value, label]) => h('a', { href: `${base}/${value}`, class: tab === value ? 'on' : undefined }, label))),
    h('div', { id: 'message' }),
    body,
  )
  if (tab === 'keys') return keysTab(tenantId, body)
  if (tab === 'settings') return settingsTab(tenantId, body)
  return queuesTab(tenantId, body)
}

async function queuesTab(tenant, body) {
  const rows = h('tbody')
  body.replaceChildren(h('table', {},
    h('thead', {}, h('tr', {}, h('th', {}, 'Queue'), h('th', { class: 'num' }, 'Ready'), h('th', { class: 'num' }, 'Delayed'),
      h('th', { class: 'num' }, 'Processing'), h('th', { class: 'num' }, 'Dead-letter'), h('th', { class: 'num' }, 'Oldest ready'), h('th', {}, ''))),
    rows))

  async function fill() {
    const { items } = await call('GET', '/v1/queues', { tenant })
    rows.replaceChildren(...items.map((q) => h('tr', {},
      h('td', {}, h('a', { href: `#/tenant/${enc(tenant)}/queue/${enc(q.name)}` }, q.name), q.settings.paused ? h('span', { class: 'muted' }, ' (paused)') : ''),
      h('td', { class: 'num' }, n(q.stats.ready)),
      h('td', { class: 'num' }, n(q.stats.delayed)),
      h('td', { class: 'num' }, n(q.stats.processing)),
      h('td', { class: q.stats.failed ? 'num error' : 'num' }, n(q.stats.failed)),
      h('td', { class: 'num' }, q.stats.ready ? seconds(q.stats.oldest_ready_seconds) : '-'),
      h('td', {}, h('button', {
        type: 'button',
        onclick: () => attempt(async () => {
          await call('PATCH', `/v1/queues/${enc(q.name)}`, { tenant, body: { paused: !q.settings.paused } })
          await fill()
        }),
      }, q.settings.paused ? 'Resume' : 'Pause')))))
    if (items.length === 0) rows.replaceChildren(h('tr', {}, h('td', { colspan: 7, class: 'muted' }, 'No queues yet: a queue exists from its first message on.')))
  }

  await attempt(fill)
}

async function keysTab(tenant, body) {
  const rows = h('tbody')
  const name = h('input', { required: true, maxlength: 255 })
  const scopes = ['publish', 'consume', 'admin'].map((scope) => h('label', {}, scope, h('input', { type: 'checkbox', value: scope, checked: scope !== 'admin' })))
  const queues = h('input', { placeholder: 'all, or e.g. import-* mail' })
  const created = h('div')

  body.replaceChildren(
    h('form', {
      onsubmit: async (event) => {
        event.preventDefault()
        const chosen = scopes.map((label) => label.querySelector('input')).filter((box) => box.checked).map((box) => box.value)
        const request = { name: name.value.trim(), scopes: chosen }
        const patterns = queues.value.split(/[\s,]+/).filter(Boolean)
        if (patterns.length) request.queues = patterns
        await attempt(async () => {
          const key = await call('POST', keysPath(tenant), { body: request })
          created.replaceChildren(secretBox(`Key ${key.name}:`, key.secret))
          name.value = ''
          await fill()
        })
      },
    }, h('label', {}, 'Name', name), scopes, h('label', {}, 'Queues', queues), h('button', {}, 'Create key')),
    created,
    h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Name'), h('th', {}, 'Starts with'), h('th', {}, 'Scopes'),
      h('th', {}, 'Queues'), h('th', {}, 'Last used'), h('th', {}, ''))), rows),
  )

  async function fill() {
    const { items } = await call('GET', keysPath(tenant))
    rows.replaceChildren(...items.map((key) => h('tr', {},
      h('td', {}, key.name),
      h('td', {}, h('code', {}, `${key.prefix}...`)),
      h('td', {}, key.scopes.join(', ')),
      h('td', {}, key.queues.length ? key.queues.join(', ') : 'all'),
      h('td', {}, when(key.last_used_at)),
      h('td', {}, confirmButton('Revoke', () => attempt(async () => {
        await call('DELETE', `${keysPath(tenant)}/${enc(key.id)}`)
        await fill()
      }, `Key ${key.name} revoked.`), { danger: true })))))
  }

  await attempt(fill)
}

async function settingsTab(tenantId, body) {
  if (session.mode !== 'operator') {
    body.replaceChildren(h('p', { class: 'muted' }, 'Suspending and deleting a tenant takes an operator key.'))
    return
  }
  await attempt(async () => {
    const tenant = await call('GET', `/v1/tenants/${enc(tenantId)}`)
    const defaults = h('textarea', { value: JSON.stringify(tenant.queue_defaults, null, 2), spellcheck: 'false' })
    const confirm = h('input', { placeholder: tenantId, disabled: tenant.status !== 'suspended' })
    body.replaceChildren(
      h('p', {}, `${tenant.name}, ${tenant.status}, created ${when(tenant.created_at)}.`),
      h('h2', {}, 'Queue defaults'),
      h('p', { class: 'muted' }, 'Every queue of this tenant uses these, unless the queue says otherwise. Only what you change is stored.'),
      defaults,
      h('button', {
        type: 'button',
        onclick: () => attempt(async () => {
          const edited = JSON.parse(defaults.value)
          const changed = Object.fromEntries(Object.entries(edited)
            .filter(([key, value]) => JSON.stringify(value) !== JSON.stringify(tenant.queue_defaults[key])))
          await call('PATCH', `/v1/tenants/${enc(tenantId)}`, { body: { queue_defaults: changed } })
        }, 'Saved.'),
      }, 'Save'),
      h('h2', {}, tenant.status === 'active' ? 'Suspend' : 'Resume'),
      h('p', { class: 'muted' }, 'A suspended tenant\'s keys are refused; its messages are kept.'),
      h('button', {
        type: 'button',
        onclick: () => attempt(async () => {
          await call('PATCH', `/v1/tenants/${enc(tenantId)}`, { body: { status: tenant.status === 'active' ? 'suspended' : 'active' } })
          route()
        }),
      }, tenant.status === 'active' ? 'Suspend' : 'Resume'),
      h('h2', {}, 'Delete'),
      h('p', { class: 'muted' }, 'Removes the tenant with every queue, message and key. Suspend it first, then type its id.'),
      h('form', {
        onsubmit: async (event) => {
          event.preventDefault()
          if (confirm.value !== tenantId) return message('Type the tenant id to delete it.')
          if (await attempt(() => call('DELETE', `/v1/tenants/${enc(tenantId)}`))) location.hash = '#/'
        },
      }, confirm, h('button', { class: 'danger' }, 'Delete tenant')),
    )
  })
}

// ---------------------------------------------------------------------------- one queue

async function queuePage(tenant, queue) {
  const path = `/v1/queues/${enc(queue)}`
  const status = h('select', {}, ['failed', 'pending', 'processing', 'completed'].map((s) => h('option', { value: s }, s === 'failed' ? 'failed (dead-letter)' : s)))
  const errorType = h('input', { placeholder: 'any error type' })
  const stats = h('p')
  const rows = h('tbody')
  const more = h('div')
  let cursor = null

  show(
    h('p', {}, h('a', { href: `#/tenant/${enc(tenant)}` }, `← ${tenant}`)),
    h('h1', {}, 'Queue ', h('code', {}, queue)),
    stats,
    h('div', { id: 'message' }),
    h('form', {
      onsubmit: (event) => { event.preventDefault(); attempt(() => fill(false)) },
    }, h('label', {}, 'Messages', status), h('label', {}, 'Error type', errorType), h('button', {}, 'Show'),
    confirmButton('Replay all shown status', () => bulk('replay')),
    confirmButton('Delete all shown status', () => bulk('purge'), { danger: true })),
    h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Message'), h('th', {}, 'Attempts'), h('th', {}, 'Last error'), h('th', {}, ''))), rows),
    more,
  )

  async function fillStats() {
    const q = await call('GET', path, { tenant })
    stats.replaceChildren(`${n(q.stats.ready)} ready, ${n(q.stats.delayed)} delayed, ${n(q.stats.processing)} processing, `
      + `${n(q.stats.completed)} completed, ${n(q.stats.failed)} dead-letter${q.settings.paused ? '; paused' : ''}.`)
  }

  async function fill(append) {
    const page = await call('GET', `${path}/messages`, {
      tenant,
      query: { status: status.value, error_type: errorType.value.trim(), limit: 50, cursor: append ? cursor : undefined },
    })
    cursor = page.next_cursor
    const built = page.items.map(messageRows).flat()
    if (append) rows.append(...built)
    else rows.replaceChildren(...built)
    if (!append && page.items.length === 0) rows.replaceChildren(h('tr', {}, h('td', { colspan: 4, class: 'muted' }, 'None.')))
    more.replaceChildren(cursor ? h('button', { type: 'button', onclick: () => attempt(() => fill(true)) }, 'More') : '')
    await fillStats()
  }

  function messageRows(m) {
    const detail = h('tr', { hidden: true }, h('td', { colspan: 4 },
      h('pre', {}, JSON.stringify({ body: m.body, headers: m.headers, last_error: m.last_error, failed_routes: m.failed_routes, dedup_key: m.dedup_key }, null, 2))))
    const error = m.last_error ? (m.last_error.expired ? 'lease expired' : `${m.last_error.type || 'error'}: ${m.last_error.message || ''}`) : ''
    return [
      h('tr', {},
        h('td', {}, h('a', { href: '#', onclick: (event) => { event.preventDefault(); detail.hidden = !detail.hidden } }, m.id),
          h('div', { class: 'muted' }, when(m.failed_at || m.completed_at || m.created_at))),
        h('td', {}, `${m.attempts} / ${m.max_attempts}`),
        h('td', {}, error),
        h('td', {},
          m.status === 'failed' || m.status === 'completed'
            ? h('button', { type: 'button', onclick: () => attempt(async () => { await call('POST', `/v1/messages/${enc(m.id)}/replay`, { tenant }); await fill(false) }, 'Replayed.') }, 'Replay')
            : '',
          m.status !== 'processing'
            ? confirmButton('Delete', () => attempt(async () => { await call('DELETE', `/v1/messages/${enc(m.id)}`, { tenant }); await fill(false) }, 'Deleted.'), { danger: true })
            : '')),
      detail,
    ]
  }

  async function bulk(action) {
    if (status.value !== 'failed' && status.value !== 'completed') {
      message('Bulk replay and delete work on failed or completed messages.')
      return
    }
    if (action === 'replay' && status.value !== 'failed') {
      message('Replay all works on the dead-letter set (failed).')
      return
    }
    const filter = { status: status.value, limit: 10000 }
    if (errorType.value.trim()) filter.error_type = errorType.value.trim()
    await attempt(async () => {
      const result = await call('POST', `${path}/${action}`, { tenant, body: filter })
      message(`${n(result.count)} messages ${action === 'replay' ? 'replayed' : 'deleted'}.`, 'notice')
      await fill(false)
    })
  }

  await attempt(() => fill(false))
}

// ---------------------------------------------------------------------------- routing

async function route() {
  header()
  if (!session) return connectPage()
  const parts = location.hash.replace(/^#\/?/, '').split('/').map(decodeURIComponent)
  if (parts[0] === 'tenant' && parts[2] === 'queue' && parts[3]) return queuePage(parts[1], parts[3])
  if (parts[0] === 'tenant' && parts[1]) return tenantPage(parts[1], parts[2] || 'queues')
  if (session.mode === 'tenant') return tenantPage(session.tenant, 'queues')
  return tenantsPage()
}

window.addEventListener('hashchange', route)
route()
