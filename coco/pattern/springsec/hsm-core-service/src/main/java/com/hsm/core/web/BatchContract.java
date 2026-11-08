package com.hsm.core.web;

/**
 * The correlation and partial-failure contract shared by every batch endpoint
 * (/encrypt/batch, /decrypt/batch, /dek/issue, /dek/unwrap), as published in the
 * OpenAPI documents. Enforced in the services' batch methods (duplicate-key and
 * item-count checks) and the *ResultItem records.
 */
final class BatchContract {

    static final String DESCRIPTION = """
            Each item carries a caller-supplied `key` (required, max 256 characters, unique \
            within the request). Every result echoes its item's `key`: match results to items \
            by `key`, not by position (results currently come back in request order, but that \
            is not part of the contract).

            A processed batch always returns HTTP 200. Each result has `status` `success` or \
            `error`; an error result carries a `detail` message and the other items are \
            unaffected -- check every item. The whole request is rejected (422) only for an \
            empty batch, more items than the configured limit, or a duplicate `key`.""";

    private BatchContract() {
    }
}
