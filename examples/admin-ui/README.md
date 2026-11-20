# Example admin interface

A small management interface on the public v1 API: three static files, no build step,
deliberately plain. It shows how to run a LessorMQ installation, and proves the API is
enough to do it.

- With an **operator key**: every tenant. Create one (its first admin key is shown once),
  suspend, resume and delete it, and open it to work inside it.
- Inside a tenant (with the operator key, through the `LessorMQ-Tenant` header, or with
  the tenant's own **admin key**): queues with their counts, pause and resume; API keys,
  issued and revoked; the dead-letter set per queue, filtered by error type, with replay
  and delete, one at a time or all at once; the tenant's queue defaults.

## Run it

Serve the folder from any static web server, on an origin the server allows:

```sh
php -S 127.0.0.1:8081 -t examples/admin-ui
```

Open http://127.0.0.1:8081, and connect with the server's address and a key.

The page calls the server straight from the browser, so the server must allow its origin
with CORS. `CORS_ALLOW_ORIGIN` is a regular expression; the default allows `localhost`
and `127.0.0.1` on any port. For an interface at `https://admin.example.com`:

```sh
CORS_ALLOW_ORIGIN='^https://admin\.example\.com$'
```

The key stays in the tab's `sessionStorage` and goes only to the server you connect to.
Everything the server returns is shown as text, never as HTML: message bodies and error
messages come from workers.
