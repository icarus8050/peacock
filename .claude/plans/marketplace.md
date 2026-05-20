# Claude Code Plugin Marketplace 구축

## 목표

`icarus8050/claude-plugins` GitHub repo를 Claude Code plugin marketplace로 구축. peacock의 `.claude/skills/`와 `.claude/agents/`를 plugin화해 공개.

## 결정 사항

| 항목 | 값 | 이유 |
|---|---|---|
| Marketplace repo | `icarus8050/claude-plugins` | 보편적 명명 |
| 로컬 경로 | `~/IdeaProjects/claude-plugins` | 사용자 지정 |
| Plugin 수 | 2개 | 코드 품질 점검과 회고는 사용 시점이 달라 분리 |
| Plugin 1 | `quality-review` (skills: self-review, refactor-scan + agent: code-reviewer) | 코드 변경 시 점검 묶음 |
| Plugin 2 | `retrospect` (skill: retrospect) | 세션 회고 / 메타-학습 단독 |
| License | MIT | 표준 |
| README 언어 | 한국어 | 사용자 지정 |
| peacock의 skills/agents | **이전 후 삭제** (rules는 유지) | 중복 제거 |
| peacock의 rules/ | 그대로 유지 | CLAUDE.md fragment는 plugin 스펙 밖이고 Go 프로젝트 특화 |

## Repo 구조

```
icarus8050/claude-plugins/
├── README.md                            # 한국어, /plugin marketplace add 안내
├── LICENSE                              # MIT
├── .claude-plugin/
│   └── marketplace.json                 # name, owner, plugins[2]
└── plugins/
    ├── quality-review/
    │   ├── .claude-plugin/plugin.json
    │   ├── skills/
    │   │   ├── self-review/SKILL.md     # peacock에서 verbatim 복사
    │   │   └── refactor-scan/SKILL.md   # peacock에서 verbatim 복사
    │   └── agents/
    │       └── code-reviewer.md         # "peacock 프로젝트" → "독립" 일반화
    └── retrospect/
        ├── .claude-plugin/plugin.json
        └── skills/
            └── retrospect/SKILL.md      # peacock에서 verbatim 복사
```

## marketplace.json 스키마

```json
{
  "name": "claude-plugins",
  "owner": { "name": "icarus8050" },
  "plugins": [
    { "name": "quality-review", "source": "./plugins/quality-review", "description": "..." },
    { "name": "retrospect", "source": "./plugins/retrospect", "description": "..." }
  ]
}
```

## 단계

1. **peacock에 plan 파일 기록** ← 현재
2. **`gh repo create icarus8050/claude-plugins --public`** + 로컬 clone to `~/IdeaProjects/claude-plugins`
3. **scaffold 작성** — README, LICENSE, marketplace.json, 두 plugin의 plugin.json + skill/agent 파일 복사 + code-reviewer 일반화 1줄
4. **승인 받아 commit + push** — "초기 marketplace scaffold" 1 commit
5. **peacock 정리** — `.claude/skills/`, `.claude/agents/` 삭제
6. **승인 받아 peacock commit** — "skills와 agents plugin 분리"

## 설치 검증 (작업 후)

```
/plugin marketplace add icarus8050/claude-plugins
/plugin install quality-review@claude-plugins
/plugin install retrospect@claude-plugins
```

## 재개 절차 (세션 끊기 시)

- 어디까지 진행됐는지: `gh repo view icarus8050/claude-plugins`로 repo 존재 여부, `~/IdeaProjects/claude-plugins`의 파일/`git log`로 단계 확인.
- 위 "단계" 목록 기준으로 미완료 항목부터 재개.
