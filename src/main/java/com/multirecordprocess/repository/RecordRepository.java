package com.multirecordprocess.repository;

import com.multirecordprocess.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RecordRepository extends JpaRepository<Product,Long> {
}
