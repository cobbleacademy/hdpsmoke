package com.hsm.core.repository;

import com.hsm.core.model.DevStatusItem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DevStatusItemRepository extends JpaRepository<DevStatusItem, Long> {

    List<DevStatusItem> findAllByOrderByCategoryAscIdAsc();
}
