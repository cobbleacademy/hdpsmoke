package com.hsm.fileservice.web;

/** What is known about one request so far -- filled in as the request progresses, so the audit line is complete even on failure. */
final class RequestTrace {
    String path;
    String endUser;
    String caller;
    String mode = "none";
    String fileId;
    String formatVersion = "unknown";
    String outcome = "error";
    String errorCode;
    long bytes;
}
