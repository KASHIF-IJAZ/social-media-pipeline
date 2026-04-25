import duckdb
import json
from pathlib import Path
from datetime import datetime

DB_PATH   = Path(r"E:/de_project/social-media-pipeline/data/social_media.duckdb")
OUT_PATH  = Path(r"E:/de_project/social-media-pipeline/data/dashboard.html")

con = duckdb.connect(str(DB_PATH))

# ── Pull data ──────────────────────────────────────────────────
daily = con.execute("""
    SELECT CAST(date AS VARCHAR) AS date,
           total_posts, total_likes,
           viral_posts,
           ROUND(platform_engagement_rate, 4) AS eng_rate
    FROM main_gold.mart_daily_engagement
    ORDER BY date ASC
    LIMIT 30
""").df()

hashtags = con.execute("""
    SELECT hashtag,
           ROUND(trending_score, 2) AS trending_score,
           total_posts, total_likes
    FROM main_gold.mart_hashtag_performance
    ORDER BY trending_score DESC
    LIMIT 15
""").df()

users = con.execute("""
    SELECT username, influencer_tier,
           total_posts,
           total_likes_received,
           viral_posts
    FROM main_gold.mart_user_performance
    ORDER BY total_likes_received DESC
    LIMIT 10
""").df()

tiers = con.execute("""
    SELECT influencer_tier,
           COUNT(*) AS user_count
    FROM main_gold.mart_user_performance
    GROUP BY influencer_tier
    ORDER BY user_count DESC
""").df()

kpis = con.execute("""
    SELECT COUNT(DISTINCT post_id)  AS total_posts,
           SUM(like_count)          AS total_likes,
           SUM(comment_count)       AS total_comments,
           SUM(view_count)          AS total_views,
           SUM(CAST(is_viral AS INT)) AS viral_posts
    FROM main_silver.stg_posts
""").df().iloc[0]

# ── Build HTML ─────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Social Media Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f1117; color: #e2e8f0; padding: 24px; }}
  h1   {{ font-size: 24px; font-weight: 600; margin-bottom: 4px; color: #f8fafc; }}
  .sub {{ font-size: 13px; color: #64748b; margin-bottom: 24px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 24px; }}
  .kpi {{ background: #1e2130; border: 1px solid #2d3748; border-radius: 12px;
          padding: 16px; text-align: center; }}
  .kpi-val {{ font-size: 28px; font-weight: 700; color: #60a5fa; }}
  .kpi-lbl {{ font-size: 11px; color: #64748b; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }}
  .card {{ background: #1e2130; border: 1px solid #2d3748; border-radius: 12px; padding: 20px; }}
  .card h2 {{ font-size: 14px; font-weight: 600; color: #94a3b8; margin-bottom: 16px;
              text-transform: uppercase; letter-spacing: 0.5px; }}
  .full {{ grid-column: 1 / -1; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th    {{ text-align: left; padding: 8px 12px; color: #64748b; font-weight: 500;
           border-bottom: 1px solid #2d3748; font-size: 11px; text-transform: uppercase; }}
  td    {{ padding: 8px 12px; border-bottom: 1px solid #1a1f2e; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 99px;
            font-size: 11px; font-weight: 500; }}
  .mega  {{ background: #7c3aed22; color: #a78bfa; }}
  .macro {{ background: #2563eb22; color: #60a5fa; }}
  .micro {{ background: #05966922; color: #34d399; }}
  .nano  {{ background: #d9770622; color: #fb923c; }}
  .regular {{ background: #37415122; color: #94a3b8; }}
</style>
</head>
<body>

<h1>Social Media Analytics Pipeline</h1>
<p class="sub">Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &nbsp;·&nbsp;
   Built with Python · DuckDB · dbt · Kafka · AWS S3</p>

<div class="kpi-grid">
  <div class="kpi">
    <div class="kpi-val">{int(kpis['total_posts']):,}</div>
    <div class="kpi-lbl">Total Posts</div>
  </div>
  <div class="kpi">
    <div class="kpi-val">{int(kpis['total_likes']):,}</div>
    <div class="kpi-lbl">Total Likes</div>
  </div>
  <div class="kpi">
    <div class="kpi-val">{int(kpis['total_comments']):,}</div>
    <div class="kpi-lbl">Total Comments</div>
  </div>
  <div class="kpi">
    <div class="kpi-val">{int(kpis['total_views']):,}</div>
    <div class="kpi-lbl">Total Views</div>
  </div>
  <div class="kpi">
    <div class="kpi-val">{int(kpis['viral_posts']):,}</div>
    <div class="kpi-lbl">Viral Posts</div>
  </div>
</div>

<div class="grid">

  <div class="card full">
    <h2>Daily Engagement Trend</h2>
    <canvas id="dailyChart" height="80"></canvas>
  </div>

  <div class="card">
    <h2>Top Trending Hashtags</h2>
    <canvas id="hashtagChart" height="200"></canvas>
  </div>

  <div class="card">
    <h2>Influencer Tier Distribution</h2>
    <canvas id="tierChart" height="200"></canvas>
  </div>

  <div class="card full">
    <h2>Top 10 Users by Engagement</h2>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Username</th>
          <th>Tier</th>
          <th>Posts</th>
          <th>Total Likes</th>
          <th>Viral Posts</th>
        </tr>
      </thead>
      <tbody>
        {''.join(f"""
        <tr>
          <td style="color:#64748b">{i+1}</td>
          <td style="color:#f1f5f9;font-weight:500">@{row['username']}</td>
          <td><span class="badge {row['influencer_tier']}">{row['influencer_tier']}</span></td>
          <td>{int(row['total_posts']) if row['total_posts'] else 0}</td>
          <td style="color:#60a5fa">{int(row['total_likes_received']) if row['total_likes_received'] else 0:,}</td>
          <td style="color:#f59e0b">{int(row['viral_posts']) if row['viral_posts'] else 0}</td>
        </tr>""" for i, (_, row) in enumerate(users.iterrows()))}
      </tbody>
    </table>
  </div>

</div>

<script>
const dailyLabels = {json.dumps(daily['date'].tolist())};
const dailyLikes  = {json.dumps(daily['total_likes'].tolist())};
const dailyPosts  = {json.dumps(daily['total_posts'].tolist())};
const dailyViral  = {json.dumps(daily['viral_posts'].tolist())};

new Chart(document.getElementById('dailyChart'), {{
  type: 'line',
  data: {{
    labels: dailyLabels,
    datasets: [
      {{ label: 'Total Likes', data: dailyLikes,
         borderColor: '#60a5fa', backgroundColor: '#60a5fa22',
         fill: true, tension: 0.4, pointRadius: 2 }},
      {{ label: 'Total Posts', data: dailyPosts,
         borderColor: '#34d399', backgroundColor: 'transparent',
         tension: 0.4, pointRadius: 2 }},
      {{ label: 'Viral Posts', data: dailyViral,
         borderColor: '#f59e0b', backgroundColor: 'transparent',
         tension: 0.4, pointRadius: 2, borderDash: [5,5] }}
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ labels: {{ color: '#94a3b8', font: {{ size: 12 }} }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 10 }}, grid: {{ color: '#1e293b' }} }},
      y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#1e293b' }} }}
    }}
  }}
}});

const hashLabels  = {json.dumps(hashtags['hashtag'].tolist())};
const hashScores  = {json.dumps(hashtags['trending_score'].tolist())};

new Chart(document.getElementById('hashtagChart'), {{
  type: 'bar',
  data: {{
    labels: hashLabels,
    datasets: [{{ label: 'Trending Score', data: hashScores,
      backgroundColor: '#7c3aed88', borderColor: '#7c3aed',
      borderWidth: 1, borderRadius: 4 }}]
  }},
  options: {{
    indexAxis: 'y',
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#1e293b' }} }},
      y: {{ ticks: {{ color: '#94a3b8', font: {{ size: 11 }} }}, grid: {{ color: '#1e293b' }} }}
    }}
  }}
}});

const tierLabels = {json.dumps(tiers['influencer_tier'].tolist())};
const tierCounts = {json.dumps(tiers['user_count'].tolist())};

new Chart(document.getElementById('tierChart'), {{
  type: 'doughnut',
  data: {{
    labels: tierLabels,
    datasets: [{{ data: tierCounts,
      backgroundColor: ['#7c3aed','#2563eb','#059669','#d97706','#475569'],
      borderWidth: 0 }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: 'right',
        labels: {{ color: '#94a3b8', font: {{ size: 12 }}, padding: 16 }} }}
    }}
  }}
}});
</script>
</body>
</html>"""

OUT_PATH.write_text(html, encoding="utf-8")
print("Dashboard created: " + str(OUT_PATH))
print("Open in browser: file:///" + str(OUT_PATH).replace("\\", "/"))