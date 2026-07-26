package com.hsm.encryption.repository;

import com.hsm.encryption.model.DevStatusItem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DevStatusItemRepository extends JpaRepository<DevStatusItem, Long> {

    List<DevStatusItem> findAllByOrderByCategoryAscIdAsc();
}
