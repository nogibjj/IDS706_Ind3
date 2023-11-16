SELECT major_category AS category, major, SUM(men) AS total_men, SUM(women) AS total_women, AVG(sharewomen) as avg_share_women
FROM recentgrads_delta AS a 
LEFT JOIN womenstem_delta AS b ON a.major = b.major
GROUP BY category, major
ORDER BY category, major 