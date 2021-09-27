SELECT t3.*
FROM (
         SELECT t2.*,
                rank() over ( PARTITION BY t2.area ORDER BY t2.click_count DESC ) AS rown
         FROM (
                  SELECT area,
                         product_name,
                         count(*) AS click_count
                  FROM (
                           SELECT a.*,
                                  product.product_name,
                                  city.area,
                                  city.city_name
                           FROM user_visit_action as a
                                    LEFT JOIN product_info product ON a.click_product_id = product.product_id
                                    LEFT JOIN city_info city ON a.city_id = city.city_id
                           WHERE a.click_product_id > - 1
                       ) t1
                  GROUP BY t1.area,
                           t1.product_name
              ) t2
     ) t3
WHERE t3.rown <= 3