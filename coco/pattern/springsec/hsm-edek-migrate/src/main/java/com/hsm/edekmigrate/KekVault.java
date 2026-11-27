package com.hsm.edekmigrate;

/**
 * One tenant's HSM, as far as the move needs it: unwrap an EDEK with an exact KEK
 * version, and wrap a DEK with a KEK's latest version. The KEK never leaves the HSM.
 */
public interface KekVault {

    /** The DEK. The caller zeroes the returned array when done with it. */
    byte[] unwrap(byte[] edek, String kekName, String kekVersion);

    /** The EDEK, and the KEK version the HSM used for it. */
    Wrapped wrap(byte[] dek, String kekName);

    record Wrapped(byte[] edek, String kekVersion) {
    }
}
