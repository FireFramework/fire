#!/bin/bash
#
# Fire 框架交互式编译脚本
# 用法: ./dev/script/build-fire.sh
#
# 编译成功后会将 fire-bundle-spark / fire-bundle-flink 的 shade jar
# 复制到仓库根目录下的: dev/bundle/
#

SELF=$(cd "$(dirname "$0")" && pwd)
# 脚本位于 dev/script/，仓库根目录为上两级
REPO_ROOT=$(cd "$SELF/../.." && pwd)
cd "$REPO_ROOT"

# Maven 可执行文件：MVN > M2_HOME/MAVEN_HOME > PATH 中的 mvn
if [ -n "${MVN:-}" ]; then
  :
elif [ -n "${M2_HOME:-}" ] && [ -x "${M2_HOME}/bin/mvn" ]; then
  MVN="${M2_HOME}/bin/mvn"
elif [ -n "${MAVEN_HOME:-}" ] && [ -x "${MAVEN_HOME}/bin/mvn" ]; then
  MVN="${MAVEN_HOME}/bin/mvn"
else
  MVN="mvn"
fi

# settings.xml：仅当环境变量显式提供时才传 -s
MVN_SETTINGS="${MVN_SETTINGS:-}"

# 本地仓库：仅当环境变量显式提供时才覆盖 Maven 默认
MAVEN_REPO_LOCAL="${MAVEN_REPO_LOCAL:-${MVN_TEMP_REPO:-}}"

# 公共 profile：同时编 Spark / Flink
COMMON_PROFILES="hadoop-2.7,hudi-0.9,paimon-1.1.1,build-spark,build-flink,fire-plus"
if [ -n "${EXTRA_PROFILES:-}" ]; then
  COMMON_PROFILES="${COMMON_PROFILES},${EXTRA_PROFILES}"
fi

# 编译模式：0=仅编译(install)，1=发布到私服(deploy + deploy-zto-repo)
DO_DEPLOY=0
MVN_GOAL="install"
# Fire 支持的全部 Spark / Flink 版本（与根 pom profiles 对齐）
# ALLOWED_SPARK=("2.3" "2.4" "3.0" "3.1" "3.2" "3.3" "3.4" "3.5")
# ALLOWED_FLINK=("1.12" "1.13" "1.14" "1.15" "1.16" "1.17" "1.18" "1.19")
ALLOWED_SPARK=("2.3" "3.0" "3.3")
ALLOWED_FLINK=("1.14" "1.19")
DEFAULT_SPARK="3.0"
DEFAULT_FLINK="1.19"

# bundle 产物汇总目录（相对仓库根目录）
BUNDLE_DIR="dev/bundle"
BUNDLE_SPARK_TARGET="fire-bundle/fire-bundle-spark/target"
BUNDLE_FLINK_TARGET="fire-bundle/fire-bundle-flink/target"

# 全量编译矩阵：与 release.sh 完全一致（scala,spark,flink）
#RELEASE_MATRIX=(
#  "scala-2.11|spark-2.3|flink-1.12"
#  "scala-2.11|spark-2.4|flink-1.13"
#  "scala-2.11|spark-2.4|flink-1.14"
#  "scala-2.12|spark-2.4|flink-1.12"
#  "scala-2.12|spark-3.0|flink-1.13"
#  "scala-2.12|spark-3.1|flink-1.14"
#  "scala-2.12|spark-3.2|flink-1.15"
#  "scala-2.12|spark-3.3|flink-1.16"
#  "scala-2.12|spark-3.3|flink-1.17"
#  "scala-2.12|spark-3.4|flink-1.18"
#  "scala-2.12|spark-3.5|flink-1.19"
#)

RELEASE_MATRIX=(
  "scala-2.11|spark-2.3|flink-1.14"
  "scala-2.12|spark-3.0|flink-1.14"
  "scala-2.12|spark-3.3|flink-1.19"
)

# 打印带时间戳的日志
log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

# 打印错误信息并以非 0 退出
die() {
  echo "ERROR: $*" >&2
  exit 1
}

# 判断目标值是否存在于给定列表中
# 参数: $1=目标值, 其余=候选列表
# 返回: 0=存在, 1=不存在
contains() {
  local needle="$1"
  shift
  local x
  for x in "$@"; do
    [ "$x" = "$needle" ] && return 0
  done
  return 1
}

# 用分隔符拼接列表，便于版本号展示
# 参数: $1=分隔符, 其余=列表元素
# 输出: 拼接后的字符串
join_by() {
  local sep="$1"
  shift
  local out="" item
  for item in "$@"; do
    if [ -z "$out" ]; then
      out="$item"
    else
      out="${out}${sep}${item}"
    fi
  done
  echo "$out"
}

# 根据 Spark 版本映射对应的 Scala Maven profile（单独编译时的默认规则）
scala_profile_for_spark() {
  case "$1" in
    2.3) echo "scala-2.11" ;;
    2.4|3.0|3.1|3.2|3.3|3.4|3.5) echo "scala-2.12" ;;
    *) die "不支持的 Spark 版本: $1" ;;
  esac
}

# 执行一次 Maven 编译
run_mvn() {
  local profiles="$1"
  local -a cmd
  cmd=("$MVN" "-DskipTests")
  if [ -n "$MAVEN_REPO_LOCAL" ]; then
    cmd+=("-Dmaven.repo.local=$MAVEN_REPO_LOCAL")
  fi
  if [ -n "$MVN_SETTINGS" ]; then
    cmd+=(-s "$MVN_SETTINGS")
  fi
  cmd+=(clean "$MVN_GOAL" "-P${profiles}" -U -T40C)
  log "执行: ${cmd[*]}"
  "${cmd[@]}"
}

# 按指定 Spark + Flink 版本组合编译一次
build_one() {
  local spark_ver="$1"
  local flink_ver="$2"
  local scala_p="${3:-}"
  if [ -z "$scala_p" ]; then
    scala_p=$(scala_profile_for_spark "$spark_ver")
  fi
  local profiles="${COMMON_PROFILES},${scala_p},spark-${spark_ver},flink-${flink_ver}"
  log "开始编译: Spark ${spark_ver} + Flink ${flink_ver} (${scala_p})"
  run_mvn "$profiles"
  copy_bundle_jars
  log "编译完成: Spark ${spark_ver} + Flink ${flink_ver}"
}

# 将本次编译产出的 fire-bundle-spark / fire-bundle-flink jar 复制到 BUNDLE_DIR
copy_bundle_jars() {
  local src dest_name
  mkdir -p "$BUNDLE_DIR"
  for src in \
    "$BUNDLE_SPARK_TARGET"/fire-bundle-spark_*.jar \
    "$BUNDLE_FLINK_TARGET"/fire-bundle-flink_*.jar
  do
    [ -f "$src" ] || continue
    dest_name=$(basename "$src")
    case "$dest_name" in
      original-*) continue ;;
      *-sources.jar|*-javadoc.jar|*-tests.jar) continue ;;
    esac
    cp -f "$src" "$BUNDLE_DIR/$dest_name"
    log "已复制 bundle: $dest_name -> ${BUNDLE_DIR}/"
  done
}

# 初始化 bundle 输出目录（清理历史 fire-bundle jar，避免混入旧版本）
prepare_bundle_dir() {
  mkdir -p "$BUNDLE_DIR"
  # rm -f "$BUNDLE_DIR"/fire-bundle-spark_*.jar "$BUNDLE_DIR"/fire-bundle-flink_*.jar
  log "bundle 输出目录: ${PWD}/${BUNDLE_DIR}"
}

# 全量编译：按 release.sh 的版本矩阵逐一编译（非笛卡尔积）
build_all() {
  local entry scala_p spark_p flink_p spark_ver flink_ver
  log "开始全量编译（与 release.sh 矩阵一致，共 ${#RELEASE_MATRIX[@]} 组）..."
  for entry in "${RELEASE_MATRIX[@]}"; do
    IFS='|' read -r scala_p spark_p flink_p <<< "$entry"
    spark_ver="${spark_p#spark-}"
    flink_ver="${flink_p#flink-}"
    build_one "$spark_ver" "$flink_ver" "$scala_p"
  done
  log "全量编译完成"
}


echo "=========================================="
echo "  Fire 框架编译"
echo "  支持 Spark: $(join_by ' / ' "${ALLOWED_SPARK[@]}")"
echo "  支持 Flink: $(join_by ' / ' "${ALLOWED_FLINK[@]}")"
echo "  默认: Spark ${DEFAULT_SPARK} + Flink ${DEFAULT_FLINK}"
echo "  Maven: $MVN"
echo "  bundle 输出: ${BUNDLE_DIR}"
echo "=========================================="
echo

# 兼容 zsh / bash（与 release.sh 相同写法）
echo -n "仅编译or发布到私服（B=仅编译 / D=发布）？[默认: B] " && read DEPLOY_INPUT
DEPLOY_INPUT=$(echo "${DEPLOY_INPUT:-B}" | tr '[:lower:]' '[:upper:]')
case "$DEPLOY_INPUT" in
  D|DEPLOY|Y)
    DO_DEPLOY=1
    MVN_GOAL="deploy"
    COMMON_PROFILES="${COMMON_PROFILES},deploy-zto-repo"
    log "已选择: 发布到私服（clean deploy + deploy-zto-repo）"
    ;;
  *)
    DO_DEPLOY=0
    MVN_GOAL="install"
    log "已选择: 仅编译（clean install）"
    ;;
esac
echo

# 2) 全量 or 单独版本
echo -n "一次编译所有版本还是选择指定版本编译（Y/N）？[默认: N] " && read BUILD_ALL_INPUT
BUILD_ALL_INPUT=$(echo "${BUILD_ALL_INPUT:-N}" | tr '[:lower:]' '[:upper:]')

# 仅在显式指定本地仓时准备目录；不清理默认 ~/.m2，避免误删编译机缓存
if [ -n "$MAVEN_REPO_LOCAL" ]; then
  mkdir -p "$MAVEN_REPO_LOCAL"
  rm -rf "$MAVEN_REPO_LOCAL/com/zto/fire"
fi
prepare_bundle_dir

set -e

if [ "$BUILD_ALL_INPUT" = "Y" ]; then
  log "已选择: 全量编译"
  build_all
else
  log "已选择: 单独编译"

  echo -n "请选择编译 Spark 的版本 ($(join_by ' / ' "${ALLOWED_SPARK[@]}")) [默认: ${DEFAULT_SPARK}]: " && read SPARK_INPUT
  SPARK_VER="${SPARK_INPUT:-$DEFAULT_SPARK}"
  contains "$SPARK_VER" "${ALLOWED_SPARK[@]}" || die "不支持的 Spark 版本: ${SPARK_VER}（可选: $(join_by ' / ' "${ALLOWED_SPARK[@]}")）"

  echo -n "请选择编译 Flink 的版本 ($(join_by ' / ' "${ALLOWED_FLINK[@]}")) [默认: ${DEFAULT_FLINK}]: " && read FLINK_INPUT
  FLINK_VER="${FLINK_INPUT:-$DEFAULT_FLINK}"
  contains "$FLINK_VER" "${ALLOWED_FLINK[@]}" || die "不支持的 Flink 版本: ${FLINK_VER}（可选: $(join_by ' / ' "${ALLOWED_FLINK[@]}")）"

  echo
  log "确认编译组合: Spark ${SPARK_VER} + Flink ${FLINK_VER}"
  build_one "$SPARK_VER" "$FLINK_VER"
fi