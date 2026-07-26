package com.hsm.encryption.repository;

import com.hsm.encryption.model.AppRegistration;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRegistrationRepository extends JpaRepository<AppRegistration, String> {
}
