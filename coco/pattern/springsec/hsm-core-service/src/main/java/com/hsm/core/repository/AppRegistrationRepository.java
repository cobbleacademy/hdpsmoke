package com.hsm.core.repository;

import com.hsm.core.model.AppRegistration;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRegistrationRepository extends JpaRepository<AppRegistration, String> {
}
