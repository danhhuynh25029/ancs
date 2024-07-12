run:
	docker compose up -d
topics:
	docker compose exec redpanda rpk topic list
topics_msg:
	docker compose exec redpanda rpk topic consume dbz.shops.orders 