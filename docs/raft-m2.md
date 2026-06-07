# Raft M2 데모 — Snapshot

M2 마일스톤(snapshot + 로그 압축 + InstallSnapshot catch-up)을 굴려보는 가이드. M1 데모([`raft-m1.md`](raft-m1.md))의 연장이며, 같은 `cmd/peacockd-demo` 바이너리에 `--snapshot-threshold` 플래그가 추가됐다. state machine은 여전히 Normal entry 카운트만 올리는 mock(`demoSM`)이고, 그 카운트가 snapshot으로 직렬화/복원된다.

관련 문서:
- 설계 청사진: [`.claude/plans/raft.md`](../.claude/plans/raft.md)
- M1 기본 동작(빌드·propose·장애): [`raft-m1.md`](raft-m1.md)

---

## 핵심 개념

- **`--snapshot-threshold N`**: 마지막 snapshot 이후 적용된 entry가 N개 이상이면 노드가 SM 상태를 snapshot으로 영속하고 로그의 그 prefix를 버린다(`$DIR/snap/`). `0`(기본)이면 비활성 — 로그가 무한히 자란다(M1 동작).
- **InstallSnapshot**: leader가 압축으로 더 이상 갖고 있지 않은 옛 entry를 필요로 하는 follower에게는, entry 대신 snapshot을 통째로 스트리밍해 한 번에 따라잡힌다.
- **재기동 복원**: snapshot이 있는 dir로 재기동하면 SM이 snapshot에서 복원되고 로그는 그 다음부터 재생된다.

## 빌드

```bash
go build -o bin/peacockd-demo ./cmd/peacockd-demo
```

## 시나리오 1 — snapshot 발동 + 압축

3노드를 `--snapshot-threshold=8`로 띄운다(M1과 동일하되 플래그 추가). 예시는 node-1만, 나머지는 `--id/--raft-addr/--http-addr/--dir`만 바꿔 동일하게.

```bash
./bin/peacockd-demo \
  --id=node-1 --raft-addr=127.0.0.1:4001 --http-addr=127.0.0.1:5001 \
  --dir=./raft-data/node-1 --snapshot-threshold=8 \
  --peers=node-1=127.0.0.1:4001,node-2=127.0.0.1:4002,node-3=127.0.0.1:4003
```

leader 포트를 찾고([`raft-m1.md`](raft-m1.md) 참고) threshold를 넘게 propose한다.

```bash
LEADER=5001
for i in $(seq 1 20); do
  curl -s -X POST --data-binary "v$i" http://127.0.0.1:$LEADER/propose >/dev/null
done

for p in 5001 5002 5003; do
  printf "%d  " "$p"; curl -s http://127.0.0.1:$p/status
done
# 세 노드 모두 normalApplied=20 이면 정상. 내부적으로 각 노드는 8개 임계마다
# snapshot을 만들고 로그 prefix를 버렸다($DIR/snap/ 에 snap-meta-*/snap-data-* 생성).
```

`$DIR/snap/` 디렉터리를 보면 최신 snapshot 1쌍(meta+data)만 남아 있다.

```bash
ls ./raft-data/node-1/snap/
# snap-meta-I0000000000000000NN-T...  snap-data-I0000000000000000NN-T...
```

## 시나리오 2 — 뒤처진 노드의 InstallSnapshot catch-up

압축이 일어난 뒤 빈 노드가 합류하면, leader는 사라진 옛 entry 대신 snapshot으로 따라잡힌다.

1. **node-1, node-2만** 띄운다(quorum 2/3). node-3은 아직.
2. threshold를 크게 넘게 propose(위 시나리오 1의 루프, 예: 20회).
3. 이제 node-3을 **빈 dir로** 띄운다.

```bash
./bin/peacockd-demo \
  --id=node-3 --raft-addr=127.0.0.1:4003 --http-addr=127.0.0.1:5003 \
  --dir=./raft-data/node-3 --snapshot-threshold=8 \
  --peers=node-1=127.0.0.1:4001,node-2=127.0.0.1:4002,node-3=127.0.0.1:4003
```

node-3의 stderr에 곧 apply 로그가 몰려 찍히고, 카운트가 단번에 따라잡힌다.

```bash
curl -s http://127.0.0.1:5003/status
# normalApplied=20 — leader가 InstallSnapshot으로 상태를 통째 전송했기 때문.
# (옛 entry는 압축돼 사라졌으므로 entry replay로는 도달 불가 = snapshot 경로 입증.)
```

## 시나리오 3 — snapshot에서 재기동 복원

snapshot이 있는 노드를 죽였다 같은 dir로 다시 띄우면, 로그를 처음부터 재생하지 않고 snapshot에서 복원한 뒤 그 다음 entry만 재생한다.

```bash
# node-2를 Ctrl+C로 종료 후 같은 dir로 재기동
./bin/peacockd-demo \
  --id=node-2 --raft-addr=127.0.0.1:4002 --http-addr=127.0.0.1:5002 \
  --dir=./raft-data/node-2 --snapshot-threshold=8 \
  --peers=node-1=127.0.0.1:4001,node-2=127.0.0.1:4002,node-3=127.0.0.1:4003

curl -s http://127.0.0.1:5002/status
# normalApplied가 종료 전 값으로 복원되어 있고, 이후 leader와 동기화된다.
```

## 정리

```bash
rm -rf ./raft-data/node-1 ./raft-data/node-2 ./raft-data/node-3
```

## 알려진 한계 (M3~M4에서 해결)

- **동적 멤버십 미지원** — `--peers`는 정적. 시나리오 2의 "합류"는 사전에 peers에 포함된 노드를 늦게 띄운 것일 뿐, 진짜 AddVoter는 M3.
- **leader hint 없음** — follower로 propose하면 503만. M4.
- **KV 미통합** — payload는 mock SM 카운트. 실제 Put/Get은 M4.
- **follower 수신측 snapshot 버퍼링** — InstallSnapshot 수신 시 전체를 메모리에 모은다(작은 snapshot 가정). 디스크 스트리밍은 후순위.
