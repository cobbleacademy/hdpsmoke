package com.hsm.client.file;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers AzureBlobFileStore.parse() only -- the one part of this class
 * testable without a real Azure account/credentials. list()/openRead()/
 * openWrite() genuinely need a live storage account and aren't covered here.
 */
class AzureBlobFileStoreTest {

    @Test
    void parse_accountContainerAndPath() {
        var parsed = AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net/mycontainer/some/nested/path");
        assertThat(parsed.accountHost()).isEqualTo("myaccount.blob.core.windows.net");
        assertThat(parsed.container()).isEqualTo("mycontainer");
        assertThat(parsed.path()).isEqualTo("some/nested/path");
    }

    @Test
    void parse_containerRoot_pathIsEmpty() {
        var parsed = AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net/mycontainer");
        assertThat(parsed.accountHost()).isEqualTo("myaccount.blob.core.windows.net");
        assertThat(parsed.container()).isEqualTo("mycontainer");
        assertThat(parsed.path()).isEmpty();
    }

    @Test
    void parse_containerRootWithTrailingSlash_pathIsEmpty() {
        var parsed = AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net/mycontainer/");
        assertThat(parsed.path()).isEmpty();
    }

    @Test
    void parse_trailingSlashesOnPath_areStripped() {
        var parsed = AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net/mycontainer/some/path///");
        assertThat(parsed.path()).isEqualTo("some/path");
    }

    @Test
    void parse_noPathAtAll_throws() {
        assertThatThrownBy(() -> AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parse_emptyContainer_throws() {
        assertThatThrownBy(() -> AzureBlobFileStore.parse("https://myaccount.blob.core.windows.net//"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
