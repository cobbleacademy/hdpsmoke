package com.hsm.bulk.repository;

import com.hsm.bulk.model.EdekRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/** Duplicated (trimmed) from com.hsm.core.repository.EdekRecordRepository -- no rotation-paging/demo-listing methods, this module doesn't need them. */
public interface EdekRecordRepository extends JpaRepository<EdekRecord, UUID> {
}
