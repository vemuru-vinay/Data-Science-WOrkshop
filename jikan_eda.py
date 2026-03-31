"""
Jikan Anime EDA — Load, Merge, Clean, Explore & Plot
Loads all 7 tables from anime.db, merges into DataFrames,
then runs exploratory analysis with 10 visualisations.
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
Path("plots").mkdir(exist_ok=True)

DB = "anime.db"

# ══════════════════════════════════════════════════════════════════════
# 1. LOAD ALL TABLES
# ══════════════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB)

df_anime       = pd.read_sql("SELECT * FROM anime",              conn)
df_genres      = pd.read_sql("SELECT * FROM anime_genres",       conn)
df_themes      = pd.read_sql("SELECT * FROM anime_themes",       conn)
df_demo        = pd.read_sql("SELECT * FROM anime_demographics", conn)
df_characters  = pd.read_sql("SELECT * FROM anime_characters",   conn)
df_staff       = pd.read_sql("SELECT * FROM anime_staff",        conn)
df_stats       = pd.read_sql("SELECT * FROM anime_statistics",   conn)

conn.close()

print("Tables loaded:")
for name, d in [
    ("anime",            df_anime),
    ("anime_genres",     df_genres),
    ("anime_themes",     df_themes),
    ("anime_demographics", df_demo),
    ("anime_characters", df_characters),
    ("anime_staff",      df_staff),
    ("anime_statistics", df_stats),
]:
    print(f"  {name:<25} {d.shape}")

# ══════════════════════════════════════════════════════════════════════
# 2. CLEAN & PREPARE MAIN DATAFRAME
# ══════════════════════════════════════════════════════════════════════

# Aggregate genres per anime into one string
genres_agg = (
    df_genres.groupby("mal_id")["genre_name"]
    .agg(lambda x: ", ".join(sorted(x)))
    .reset_index()
    .rename(columns={"genre_name": "genres"})
)

# Aggregate demographics per anime
demo_agg = (
    df_demo.groupby("mal_id")["demographic_name"]
    .agg(lambda x: ", ".join(sorted(x)))
    .reset_index()
    .rename(columns={"demographic_name": "demographic"})
)

# Get director name per anime
directors = df_staff[df_staff["position"] == "Director"][["mal_id", "person_name"]].copy()
directors = directors.rename(columns={"person_name": "director"})
directors = directors.drop_duplicates(subset="mal_id")

# Merge everything into one big DataFrame
df = (
    df_anime
    .merge(genres_agg,  on="mal_id", how="left")
    .merge(demo_agg,    on="mal_id", how="left")
    .merge(directors,   on="mal_id", how="left")
    .merge(df_stats,    on="mal_id", how="left")
)

# Clean numeric columns
df["score"]      = pd.to_numeric(df["score"],      errors="coerce")
df["members"]    = pd.to_numeric(df["members"],     errors="coerce")
df["favorites"]  = pd.to_numeric(df["favorites"],   errors="coerce")
df["episodes"]   = pd.to_numeric(df["episodes"],    errors="coerce")
df["year"]       = pd.to_numeric(df["year"],         errors="coerce")
df["completed"]  = pd.to_numeric(df["completed"],   errors="coerce")
df["dropped"]    = pd.to_numeric(df["dropped"],     errors="coerce")
df["watching"]   = pd.to_numeric(df["watching"],    errors="coerce")

# Derived columns
df["completion_rate"] = (df["completed"] / df["total"] * 100).round(2)
df["drop_rate"]       = (df["dropped"]   / df["total"] * 100).round(2)

print(f"\nMerged DataFrame: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

print(f"\nNull counts in key columns:")
for col in ["score", "members", "genres", "type", "studio", "director", "year"]:
    print(f"  {col:<20} {df[col].isna().sum()}")

# ══════════════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════════════
def savefig(name):
    plt.tight_layout()
    plt.savefig(f"plots/{name}.png", dpi=150, bbox_inches="tight")
    print(f"  Saved → plots/{name}.png")
    plt.close()

# ══════════════════════════════════════════════════════════════════════
# EDA PLOTS
# ══════════════════════════════════════════════════════════════════════

# ── Q1: Which genres have the most anime? ────────────────────────────
print("\nQ1: Genre distribution")
genre_counts = df_genres["genre_name"].value_counts().head(15)

fig, ax = plt.subplots(figsize=(10, 7))
genre_counts.plot.barh(
    ax=ax,
    color=sns.color_palette("viridis", len(genre_counts))
)
ax.set_xlabel("Number of Anime")
ax.set_title("Top 15 Genres by Anime Count")
ax.invert_yaxis()
for i, val in enumerate(genre_counts):
    ax.text(val + 0.3, i, str(val), va="center", fontsize=9)
savefig("01_genre_distribution")

# ── Q2: Top rated anime of all time ──────────────────────────────────
print("Q2: Top rated anime")
top_rated = df.dropna(subset=["score"]).nlargest(15, "score")

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(
    top_rated["title"].str[:40],
    top_rated["score"],
    color=sns.color_palette("YlOrRd_r", len(top_rated))
)
ax.set_xlabel("Score")
ax.set_title("Top 15 Highest Rated Anime")
ax.set_xlim(8.5, 9.5)
ax.invert_yaxis()
for i, val in enumerate(top_rated["score"]):
    ax.text(val + 0.005, i, f"{val:.2f}", va="center", fontsize=9)
savefig("02_top_rated_anime")

# ── Q3: TV vs Movie vs OVA — which rates higher? ─────────────────────
print("Q3: Type vs Rating")
type_data = df.dropna(subset=["score", "type"])
top_types = type_data["type"].value_counts().head(5).index
type_filtered = type_data[type_data["type"].isin(top_types)]

fig, ax = plt.subplots(figsize=(10, 6))
order = type_filtered.groupby("type")["score"].median().sort_values(ascending=False).index
sns.boxplot(
    data=type_filtered,
    x="type", y="score",
    order=order,
    ax=ax,
    palette="Set2",
    showfliers=False
)
ax.set_title("Score Distribution by Anime Type (TV / Movie / OVA etc.)")
ax.set_xlabel("Type")
ax.set_ylabel("Score")
savefig("03_type_vs_rating")

# ── Q4: Which studio produces the best anime? ────────────────────────
print("Q4: Studio analysis")
studio_stats = (
    df.dropna(subset=["studio", "score"])
    .groupby("studio")
    .agg(count=("mal_id", "count"), avg_score=("score", "mean"))
    .query("count >= 3")
    .sort_values("avg_score", ascending=False)
    .head(15)
)

fig, ax = plt.subplots(figsize=(10, 7))
ax.barh(
    studio_stats.index,
    studio_stats["avg_score"],
    color=sns.color_palette("coolwarm", len(studio_stats))
)
ax.set_xlabel("Average Score")
ax.set_title("Top 15 Studios by Average Score (min 3 anime)")
ax.invert_yaxis()
ax.set_xlim(7.5, 9.5)
for i, (val, cnt) in enumerate(zip(studio_stats["avg_score"], studio_stats["count"])):
    ax.text(val + 0.02, i, f"{val:.2f} ({cnt} anime)", va="center", fontsize=9)
savefig("04_studio_analysis")

# ── Q5: Which season has best anime? ─────────────────────────────────
print("Q5: Best season")
season_data = df.dropna(subset=["season", "score"])
season_order = ["spring", "summer", "fall", "winter"]
season_data = season_data[season_data["season"].isin(season_order)]

season_stats = (
    season_data.groupby("season")
    .agg(avg_score=("score", "mean"), count=("mal_id", "count"))
    .reindex(season_order)
    .reset_index()
)

fig, ax1 = plt.subplots(figsize=(10, 5))
colors = ["#FF6B6B", "#FFD93D", "#6BCB77", "#4D96FF"]
bars = ax1.bar(
    season_stats["season"],
    season_stats["avg_score"],
    color=colors,
    alpha=0.85,
    edgecolor="white",
    linewidth=1.5
)
ax1.set_ylabel("Average Score")
ax1.set_title("Average Anime Score by Season")
ax1.set_ylim(7.5, 9)

ax2 = ax1.twinx()
ax2.plot(
    season_stats["season"],
    season_stats["count"],
    "o--", color="black", linewidth=2, markersize=8, label="Anime Count"
)
ax2.set_ylabel("Number of Anime")

for bar, val in zip(bars, season_stats["avg_score"]):
    ax1.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 0.01,
        f"{val:.2f}", ha="center", va="bottom", fontsize=10, fontweight="bold"
    )

lines, labels = ax2.get_legend_handles_labels()
ax2.legend(lines, labels, loc="upper right")
savefig("05_best_season")

# ── Q6: Shounen vs Seinen vs Shoujo ratings ──────────────────────────
print("Q6: Demographics comparison")
demo_expanded = (
    df_demo.merge(df_anime[["mal_id", "score"]], on="mal_id")
    .dropna(subset=["score"])
)
top_demos = demo_expanded["demographic_name"].value_counts().head(4).index
demo_filtered = demo_expanded[demo_expanded["demographic_name"].isin(top_demos)]

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Box plot
sns.boxplot(
    data=demo_filtered,
    x="demographic_name", y="score",
    ax=axes[0], palette="pastel", showfliers=False
)
axes[0].set_title("Score Distribution by Demographic")
axes[0].set_xlabel("")
axes[0].set_ylabel("Score")

# Count plot
demo_counts = demo_filtered["demographic_name"].value_counts()
axes[1].bar(
    demo_counts.index,
    demo_counts.values,
    color=sns.color_palette("pastel", len(demo_counts))
)
axes[1].set_title("Number of Anime by Demographic")
axes[1].set_xlabel("")
axes[1].set_ylabel("Count")
savefig("06_demographic_comparison")

# ── Q7: Most popular themes ───────────────────────────────────────────
print("Q7: Most popular themes")
theme_counts = df_themes["theme_name"].value_counts().head(15)

fig, ax = plt.subplots(figsize=(10, 7))
theme_counts.plot.barh(
    ax=ax,
    color=sns.color_palette("magma", len(theme_counts))
)
ax.set_xlabel("Number of Anime")
ax.set_title("Top 15 Most Common Anime Themes")
ax.invert_yaxis()
for i, val in enumerate(theme_counts):
    ax.text(val + 0.1, i, str(val), va="center", fontsize=9)
savefig("07_popular_themes")

# ── Q8: Score vs Members — does popularity = quality? ────────────────
print("Q8: Score vs Members")
scatter_data = df.dropna(subset=["score", "members"])

fig, ax = plt.subplots(figsize=(10, 7))
scatter = ax.scatter(
    scatter_data["members"] / 1e6,
    scatter_data["score"],
    alpha=0.6,
    c=scatter_data["score"],
    cmap="RdYlGn",
    s=40,
    edgecolors="white",
    linewidth=0.5
)
plt.colorbar(scatter, label="Score")
ax.set_xlabel("Members (Millions)")
ax.set_ylabel("Score")
ax.set_title("Score vs Members — Does Popularity = Quality?")

# Add correlation annotation
corr = scatter_data[["score", "members"]].corr().iloc[0, 1]
ax.text(
    0.05, 0.95,
    f"Correlation: {corr:.2f}",
    transform=ax.transAxes,
    fontsize=11,
    verticalalignment="top",
    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5)
)
savefig("08_score_vs_members")

# ── Q9: Completed vs Dropped ratio — engagement analysis ─────────────
print("Q9: Completed vs Dropped")
engage_data = df.dropna(subset=["completion_rate", "drop_rate", "score"])
engage_data = engage_data[engage_data["total"] > 1000]

top10_completed = engage_data.nlargest(10, "completion_rate")[["title", "completion_rate"]]
top10_dropped   = engage_data.nlargest(10, "drop_rate")[["title", "drop_rate"]]

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Most completed
axes[0].barh(
    top10_completed["title"].str[:35],
    top10_completed["completion_rate"],
    color="#6BCB77"
)
axes[0].set_xlabel("Completion Rate (%)")
axes[0].set_title("Top 10 — Highest Completion Rate")
axes[0].invert_yaxis()

# Most dropped
axes[1].barh(
    top10_dropped["title"].str[:35],
    top10_dropped["drop_rate"],
    color="#FF6B6B"
)
axes[1].set_xlabel("Drop Rate (%)")
axes[1].set_title("Top 10 — Highest Drop Rate")
axes[1].invert_yaxis()
savefig("09_completion_vs_dropped")

# ── Q10: Most favorited characters overall ───────────────────────────
print("Q10: Most favorited characters")
top_chars = (
    df_characters
    .dropna(subset=["favorites"])
    .sort_values("favorites", ascending=False)
    .head(15)
)

# Add anime title
top_chars = top_chars.merge(df_anime[["mal_id", "title"]], on="mal_id", how="left")
top_chars["label"] = top_chars["character_name"] + "\n(" + top_chars["title"].str[:20] + ")"

fig, ax = plt.subplots(figsize=(12, 8))
ax.barh(
    top_chars["label"],
    top_chars["favorites"] / 1000,
    color=sns.color_palette("flare", len(top_chars))
)
ax.set_xlabel("Favorites (thousands)")
ax.set_title("Top 15 Most Favorited Anime Characters")
ax.invert_yaxis()
for i, val in enumerate(top_chars["favorites"]):
    ax.text(val / 1000 + 0.5, i, f"{val:,}", va="center", fontsize=8)
savefig("10_top_characters")

# ══════════════════════════════════════════════════════════════════════
# SUMMARY STATISTICS
# ══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SUMMARY STATISTICS")
print("=" * 60)
print(f"Total anime            : {len(df):,}")
print(f"Average score          : {df['score'].mean():.2f}")
print(f"Highest score          : {df['score'].max():.2f} — {df.loc[df['score'].idxmax(), 'title']}")
print(f"Most members           : {df['members'].max():,.0f} — {df.loc[df['members'].idxmax(), 'title']}")
print(f"Unique studios         : {df['studio'].nunique():,}")
print(f"Unique directors       : {df['director'].nunique():,}")
print(f"Total characters       : {len(df_characters):,}")
print(f"Most common type       : {df['type'].value_counts().index[0]}")
print(f"Most common genre      : {df_genres['genre_name'].value_counts().index[0]}")
print(f"Year range             : {int(df['year'].min())} – {int(df['year'].max())}")
print(f"\nAll 10 plots saved to plots/ folder.")
print("Done! 🎉")
