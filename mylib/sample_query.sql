SELECT a.major_category AS category, a.major, SUM(a.men) AS total_men, 
           SUM(a.women) AS total_women, AVG(a.sharewomen) as avg_share_women
    FROM womenstem_delta AS a 
    INNER JOIN recentgrads_delta AS b ON a.major = b.major
    GROUP BY a.major_category, a.major
    ORDER BY a.major_category, a.major 