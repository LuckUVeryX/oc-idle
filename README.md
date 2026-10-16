# oc-idle

Compacts OpenCode sessions after 54 idle minutes when their token estimate exceeds 200,000.
Requires OpenCode 2.0.30 or later with Effect v4 support.

## Install

```sh
opencode plugin add github:LuckUVeryX/oc-idle
```

Or configure options in `opencode.jsonc`:

```jsonc
{
	"$schema": "https://opencode.ai/config.json",
	"plugins": [
		{
			"package": "github:LuckUVeryX/oc-idle",
			"options": {
				"idleMinutes": 54,
				"minTokens": 200000
			}
		}
	]
}
```

Both options are optional and accept finite, non-negative numbers.
Set `idleMinutes` to `0` for no delay, or `minTokens` to `0` to qualify any positive estimate.
New activity cancels the session's timer. Child sessions are skipped.

## Develop

Use Bun 1.4.2 or later.

```sh
bun install --frozen-lockfile
bun run check
bun run test
```
