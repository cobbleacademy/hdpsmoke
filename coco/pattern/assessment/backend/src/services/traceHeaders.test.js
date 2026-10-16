'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { filterHeaders, extractTraceIds, getTraceNames, normaliseTraceName } = require('./traceHeaders');

test('every spelling of request id normalises to the same name', () => {
  for (const n of ['requestid', 'x-request-id', 'request_id', 'X-Request-ID', 'Request-Id', 'X-RequestId']) {
    assert.equal(normaliseTraceName(n), 'requestid', n);
  }
});

test('filterHeaders drops sensitive headers and lowercases names', () => {
  const h = filterHeaders([
    ['Content-Type', 'application/json'],
    ['Set-Cookie', 'sid=abc'],
    ['Authorization', 'Bearer x'],
    ['WWW-Authenticate', 'Basic'],
    ['Proxy-Authenticate', 'Basic'],
    ['Proxy-Connection', 'keep-alive'],
    ['RequestId', 'r-1'],
  ]);
  assert.deepEqual(h, { 'content-type': 'application/json', requestid: 'r-1' });
});

test('filterHeaders joins array values and repeated names', () => {
  assert.deepEqual(filterHeaders(Object.entries({ via: ['a', 'b'] })), { via: 'a, b' });
  assert.deepEqual(filterHeaders([['x-a', '1'], ['X-A', '2']]), { 'x-a': '1, 2' });
});

test('matches requestid, x-request-id and request_id as Request ID', () => {
  for (const header of ['requestid', 'x-request-id', 'request_id']) {
    const ids = extractTraceIds({ [header]: 'abc' });
    assert.deepEqual(ids, [{ label: 'Request ID', header, value: 'abc' }]);
  }
});

test('keeps both spellings when values differ (provider + gateway)', () => {
  const ids = extractTraceIds({ requestid: 'prov-1', 'x-request-id': 'gw-2' });
  assert.deepEqual(ids.map(i => i.value), ['prov-1', 'gw-2']);
});

test('shows one chip when two spellings carry the same value', () => {
  const ids = extractTraceIds({ requestid: 'same', 'x-request-id': 'same' });
  assert.equal(ids.length, 1);
});

test('finds transaction and correlation ids, in trace-name order', () => {
  const ids = extractTraceIds({ 'x-correlation-id': 'c', transaction_id: 't', 'x-request-id': 'r', 'content-type': 'x' });
  assert.deepEqual(ids.map(i => i.label), ['Request ID', 'Transaction ID', 'Correlation ID']);
});

test('ignores empty values and unrelated headers', () => {
  assert.deepEqual(extractTraceIds({ requestid: '', 'content-type': 'json' }), []);
});

test('PROVIDER_TRACE_HEADERS overrides defaults, any spelling accepted', () => {
  assert.deepEqual(getTraceNames('x-trace-id, Span_Id'), ['traceid', 'spanid']);
  assert.deepEqual(getTraceNames(''), ['requestid', 'transactionid', 'correlationid']);
  assert.deepEqual(getTraceNames(undefined), ['requestid', 'transactionid', 'correlationid']);
  const ids = extractTraceIds({ 'x-trace-id': 't1', requestid: 'r' }, getTraceNames('trace_id'));
  assert.deepEqual(ids, [{ label: 'traceid', header: 'x-trace-id', value: 't1' }]);
});
