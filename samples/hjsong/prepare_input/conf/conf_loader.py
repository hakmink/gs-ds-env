"""
conf_loader.py
==============
env.yml 을 Source of Truth 로 삼아
*.template.yml 파일에 Jinja2 변수를 주입하고
최종 설정 dict 를 반환합니다.

사용 예시
---------
    from conf_loader import load_conf

    conf = load_conf("path/to/conf_dir")

    # 각 설정에 접근
    print(conf.env.region)                    # us-west-2
    print(conf.meta.mlflow.region_name)       # us-west-2  (주입됨)
    print(conf.meta.sagemaker.base_job_name)  # boilerplate312-sm-dev
    print(conf.meta.s3.conf_bucket)           # gs-retail-awesome-conf-us-west-2
    print(conf.meta.user_id)                  # sample     (주입됨)
    print(conf.meta.project)                  # titanic-survival-prediction

의존성
------
    pip install jinja2 pyyaml
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from jinja2 import StrictUndefined, Template, UndefinedError


# ── 버킷명 컨벤션 ────────────────────────────────────────────
# env.yml 에 s3 섹션이 명시된 경우 그 값을 우선 사용,
# 없으면 region 으로 자동 조합합니다.
_BUCKET_PREFIX = "gs-retail-awesome"

def _resolve_buckets(env: dict) -> dict[str, str]:
    """
    env 의 s3 섹션이 있으면 그 값 사용 (template 에서 이미 {{ region }} 치환됨),
    없으면 region 으로 컨벤션에 따라 자동 생성 (하위 호환 fallback).
    """
    region = env["region"]
    explicit = env.get("s3", {})

    return {
        "conf_bucket":  explicit.get("conf_bucket",
                            f"{_BUCKET_PREFIX}-conf-{region}"),
        "data_bucket":  explicit.get("data_bucket",
                            f"{_BUCKET_PREFIX}-data-{region}"),
        "model_bucket": explicit.get("model_bucket",
                            f"{_BUCKET_PREFIX}-model-{region}"),
    }


# ── 템플릿 변수 추출 ─────────────────────────────────────────
def _build_variables(env: dict) -> dict[str, Any]:
    """
    env.yml 의 최상위 스칼라 값을 모두 자동 수집하여 변수 dict 를 구성합니다.
    (str / int / float / bool / None 만 포함, dict / list 는 제외)

    ※ env.yml 에 새 스칼라 변수를 추가하면 conf_loader.py 수정 없이
      즉시 모든 *.template.yml 에서 {{ 변수명 }} 으로 사용 가능합니다.

    파생 변수 (자동 생성)
    ---------------------
    conf_bucket  / data_bucket / model_bucket
        → env.yml 의 s3: 섹션이 있으면 그 값 우선,
          없으면 region 으로 컨벤션에 따라 자동 조합.
    """
    # 1) 최상위 스칼라 전체 수집
    _SCALAR = (str, int, float, bool, type(None))
    scalars = {k: v for k, v in env.items() if isinstance(v, _SCALAR)}

    # 2) 파생 버킷명 추가 (scalars 보다 나중에 merge → override 방지)
    buckets = _resolve_buckets(env)

    return {**scalars, **buckets}


# ── Jinja2 렌더링 ────────────────────────────────────────────
def _render(template_path: Path, variables: dict[str, str]) -> dict[str, Any]:
    """
    Jinja2 로 템플릿을 렌더링한 뒤 YAML 파싱하여 반환.
    정의되지 않은 변수가 있으면 UndefinedError 로 즉시 실패 (StrictUndefined).
    """
    raw = template_path.read_text(encoding="utf-8")
    try:
        rendered = Template(raw, undefined=StrictUndefined).render(**variables)
    except UndefinedError as e:
        raise ValueError(
            f"[conf_loader] 템플릿 변수 오류 in '{template_path.name}': {e}\n"
            f"  → 사용 가능한 변수: {list(variables.keys())}"
        ) from e

    return yaml.safe_load(rendered)


# ── env.template.yml 2-pass 렌더링 ───────────────────────────
def _load_env(conf_dir: Path) -> tuple[dict, Path]:
    """
    env.template.yml 또는 env.yml 을 로드하여 완전히 렌더링된 env dict 반환.

    env.template.yml 처리 (2-pass)
    --------------------------------
    1st pass : raw YAML 파싱 → 파일 내 스칼라 값 추출 (region, env, ...)
    2nd pass : 추출한 스칼라로 Jinja2 렌더링 → {{ region }} 등 치환
               → 최종 YAML 파싱 → 완전한 env dict

    env.yml 처리 (1-pass, 하위 호환 fallback)
    ------------------------------------------
    Jinja2 변수 없이 단순 YAML 파싱.

    Returns
    -------
    (env_dict, source_path)
    """
    _SCALAR = (str, int, float, bool, type(None))

    # ── env.template.yml 우선 탐색 ──────────────────────────
    template_path = conf_dir / "env.template.yml"
    if template_path.exists():
        raw = template_path.read_text(encoding="utf-8")

        # 1st pass: Jinja2 미처리 상태로 YAML 파싱 → 스칼라 추출
        raw_parsed: dict = yaml.safe_load(raw)
        seed_vars = {k: v for k, v in raw_parsed.items() if isinstance(v, _SCALAR)}

        # 2nd pass: 스칼라 값으로 Jinja2 렌더링 → 완전한 env dict
        try:
            rendered = Template(raw, undefined=StrictUndefined).render(**seed_vars)
            print(rendered)
        except UndefinedError as e:
            raise ValueError(
                f"[conf_loader] env.template.yml 변수 오류: {e}\n"
                f"  → 1st pass 에서 추출된 변수: {list(seed_vars.keys())}"
            ) from e

        return yaml.safe_load(rendered), template_path

    # ── fallback: env.yml (하위 호환) ───────────────────────
    env_path = conf_dir / "env.yml"
    if env_path.exists():
        return yaml.safe_load(env_path.read_text(encoding="utf-8")), env_path

    raise FileNotFoundError(
        f"[conf_loader] env.template.yml / env.yml 모두 없음: {conf_dir}"
    )
def _write_yaml(data: dict, out_path: Path) -> None:
    """
    dict 를 YAML 파일로 저장합니다.
    - allow_unicode=True  : 한글 등 유니코드 그대로 출력
    - default_flow_style=False : 블록 스타일 (사람이 읽기 좋은 형태)
    - sort_keys=False     : 원본 키 순서 유지
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True,
                  default_flow_style=False, sort_keys=False)


# ── 결과를 dot-access 가능한 객체로 래핑 ────────────────────
class ConfNamespace:
    """
    dict 를 재귀적으로 dot-access 객체로 변환합니다.
    예: conf.meta.mlflow.region_name
    """
    def __init__(self, data: dict):
        for key, value in data.items():
            if isinstance(value, dict):
                setattr(self, key, ConfNamespace(value))
            else:
                setattr(self, key, value)

    def to_dict(self) -> dict:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, ConfNamespace):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def __repr__(self) -> str:
        return f"ConfNamespace({self.to_dict()})"


# ── 공개 API ─────────────────────────────────────────────────
def load_conf(
    conf_dir: str | Path,
    *,
    as_namespace: bool = True,
) -> ConfNamespace | dict:
    """
    conf_dir 안의 설정 파일을 로드하여 병합된 설정을 반환합니다.

    Parameters
    ----------
    conf_dir : str | Path
        env.yml, meta.template.yml, model.template.yml 이 위치한 디렉토리.
    as_namespace : bool
        True  → dot-access 가능한 ConfNamespace 반환  (기본값)
        False → 일반 dict 반환

    Returns
    -------
    ConfNamespace | dict
        {
          "env":   { ... },   # env.yml 원본
          "meta":  { ... },   # 렌더링된 meta
          "model": { ... },   # 렌더링된 model
          "vars":  { ... },   # 주입된 변수 (디버깅용)
        }
    """
    conf_dir = Path(conf_dir)

    # ── 1) Source of Truth 로드 (env.template.yml → 2-pass 렌더링) ──
    env, env_source = _load_env(conf_dir)

    # ── 2) 주입 변수 구성 ────────────────────────────────────
    variables = _build_variables(env)

    # ── 3) 템플릿 렌더링 ─────────────────────────────────────
    def render_if_exists(name: str) -> dict | None:
        path = conf_dir / f"{name}.template.yml"
        if not path.exists():
            return None
        return _render(path, variables)

    meta  = render_if_exists("meta")
    model = render_if_exists("model")

    # ── 4) 반환 ──────────────────────────────────────────────
    result: dict[str, Any] = {
        "env":   env,
        "meta":  meta,
        "model": model,
        "vars":  variables,   # 디버깅 / 로깅용
    }

    return ConfNamespace(result) if as_namespace else result


def generate_confs(
    conf_dir: str | Path,
    *,
    out_dir: str | Path | None = None,
) -> None:
    """
    *.template.yml 을 렌더링하여 meta.yml / model.yml 파일로 저장합니다.

    Parameters
    ----------
    conf_dir : str | Path
        env.yml, meta.template.yml, model.template.yml 이 위치한 디렉토리.
    out_dir : str | Path | None
        출력 디렉토리. None 이면 conf_dir 과 동일한 위치에 저장.

    생성 파일
    ---------
        {out_dir}/env.yml    (env.template.yml 이 있을 때만)
        {out_dir}/meta.yml
        {out_dir}/model.yml
    """
    conf_dir = Path(conf_dir)
    out_dir  = Path(out_dir) if out_dir else conf_dir

    conf = load_conf(conf_dir, as_namespace=False)
    print("=======env======",conf["env"])
    print("=======meta======",conf["meta"])
    print("=======model======",conf["model"])

    written = []

    # env.yml 생성 (env.template.yml 이 있을 때만)
    if (conf_dir / "env.template.yml").exists():
        env_out = out_dir / "env.yml"
        _write_yaml(conf["env"], env_out)
        written.append(env_out)
        print(f"[conf_loader] ✅  {env_out} 생성 완료")

    for name in ("meta", "model"):
        data = conf.get(name)
        if data is None:
            print(f"[conf_loader] ⚠️  {name}.template.yml 없음 → 건너뜀")
            continue
        out_path = out_dir / f"{name}.yml"
        _write_yaml(data, out_path)
        written.append(out_path)
        print(f"[conf_loader] ✅  {out_path} 생성 완료")

    return written


# ── CLI 실행 ─────────────────────────────────────────────────
# python conf_loader.py [conf_dir] [out_dir]
#   conf_dir : env.yml / *.template.yml 위치  (기본값: 현재 디렉토리)
#   out_dir  : 생성된 yml 저장 위치            (기본값: conf_dir 와 동일)
if __name__ == "__main__":
    import sys

    conf_dir_ = sys.argv[1] if len(sys.argv) > 1 else "."
    out_dir_  = sys.argv[2] if len(sys.argv) > 2 else conf_dir_

    generate_confs(conf_dir_, out_dir=out_dir_)
