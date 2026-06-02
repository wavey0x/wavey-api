import {all, common, createStarryNight} from '@wooorm/starry-night'
import {toHtml} from 'hast-util-to-html'

const LANGUAGE_ALIASES = new Map([
  ['sol', 'solidity'],
  ['vy', 'vyper'],
  ['py', 'python'],
  ['py3', 'python'],
  ['python3', 'python'],
  ['golang', 'go'],
  ['sh', 'shell'],
  ['bash', 'shell'],
  ['zsh', 'shell'],
  ['yml', 'yaml'],
  ['ts', 'typescript'],
  ['js', 'javascript'],
  ['rs', 'rust']
])

function normalizeFlag(value) {
  const flag = String(value || '')
    .trim()
    .split(/\s+/, 1)[0]
    .toLowerCase()

  return LANGUAGE_ALIASES.get(flag) || flag
}

function scopeToHighlightClass(scope) {
  return `highlight-${scope.replace(/[^A-Za-z0-9_.+-]/g, '-').replace(/\./g, '-')}`
}

function readStdin() {
  return new Promise((resolve, reject) => {
    let data = ''
    process.stdin.setEncoding('utf8')
    process.stdin.on('data', (chunk) => {
      data += chunk
    })
    process.stdin.on('end', () => resolve(data))
    process.stdin.on('error', reject)
  })
}

function fail(message) {
  process.stderr.write(`${message}\n`)
  process.exit(1)
}

const input = await readStdin()
let payload

try {
  payload = JSON.parse(input)
} catch {
  fail('Invalid JSON payload')
}

if (!payload || !Array.isArray(payload.blocks)) {
  fail('Payload must include a blocks array')
}

const grammars = payload.grammar_set === 'common' ? common : all
const starryNight = await createStarryNight(grammars)

const blocks = payload.blocks.map((block) => {
  const index = block && Number.isInteger(block.index) ? block.index : null
  const language = normalizeFlag(block && block.language)
  const code = typeof block?.code === 'string' ? block.code : ''
  const scope = language ? starryNight.flagToScope(language) : undefined

  if (index === null || !scope) {
    return {index, ok: false}
  }

  try {
    const tree = starryNight.highlight(code, scope)
    return {
      index,
      ok: true,
      scope,
      class_name: scopeToHighlightClass(scope),
      html: toHtml(tree)
    }
  } catch (error) {
    return {
      index,
      ok: false,
      error: error instanceof Error ? error.message : 'highlight failed'
    }
  }
})

process.stdout.write(JSON.stringify({blocks}))
