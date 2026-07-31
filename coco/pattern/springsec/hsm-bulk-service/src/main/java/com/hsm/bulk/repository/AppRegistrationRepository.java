package com.hsm.bulk.repository;

import com.hsm.bulk.model.AppRegistration;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRegistrationRepository extends JpaRepository<AppRegistration, String> {
}
