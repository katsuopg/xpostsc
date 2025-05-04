import asyncio
import sqlite3
from twscrape import API
from twscrape.logger import set_log_level
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# データベースのセットアップ
conn = sqlite3.connect('data/tweets.db')
c = conn.cursor()
c.execute('PRAGMA journal_mode=WAL;')
c.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
        id INTEGER PRIMARY KEY,   -- Tweet ID、一意
        username TEXT,
        url TEXT,
        original TEXT,
        created_at TEXT,
        like_count INTEGER DEFAULT 0,  -- いいね数
        retweet_count INTEGER DEFAULT 0,  -- リツイート数
        processed INTEGER DEFAULT 0  -- 0=未処理 1=完了
    )
''')
conn.commit()

async def save_tweets_to_db(api, user_id, account_number):
    try:
        # クッキー情報を設定
        cookies = {
            'auth_token': os.getenv(f'AUTH_TOKEN_{account_number}'),
            'ct0': os.getenv(f'CT0_TOKEN_{account_number}'),
            'other_cookie_name': os.getenv(f'OTHER_COOKIE_VALUE_{account_number}'),
        }
        
        async for tweet in api.user_tweets(user_id, limit=5):
            # リツイートを除外
            if tweet.retweetedTweet is None:
                tweet_url = f"https://twitter.com/{tweet.user.username}/status/{tweet.id}"
                created_at = tweet.date  # 正しい属性名に置き換えてください
                # データベースにすでに存在するか確認
                c.execute('SELECT 1 FROM tweets WHERE id = ?', (tweet.id,))
                if c.fetchone() is None:
                    c.execute('''
                        INSERT INTO tweets (username, id, original, url, created_at) VALUES (?, ?, ?, ?, ?)
                    ''', (user_id, tweet.id, tweet.rawContent, tweet_url, created_at))
                    conn.commit()
                    print(f"Tweet saved: {tweet.id} - {tweet.rawContent[:30]}...")  # コンソールにメッセージを表示
                else:
                    print(f"Tweet already exists: {tweet.id}")
    except Exception as e:
        print(f"Error occurred: {e}")
        await asyncio.sleep(60)  # エラー発生時は1分待機して再試行

async def main():
    set_log_level("INFO")  # ログレベルを設定

    api = API()

    # アカウントを追加
    await api.pool.add_account(
        username=os.getenv("USERNAME_1"),
        password=os.getenv("PASSWORD_1"),
        email=os.getenv("EMAIL_1"),
        email_password=os.getenv("EMAIL_PASSWORD_1"),
        cookies=os.getenv("COOKIES_1")
    )

    await api.pool.add_account(
        username=os.getenv("USERNAME_2"),
        password=os.getenv("PASSWORD_2"),
        email=os.getenv("EMAIL_2"),
        email_password=os.getenv("EMAIL_PASSWORD_2"),
        cookies=os.getenv("COOKIES_2")
    )

    # すべてのアカウントのログインを試みる
    await api.pool.login_all()

    # 監視したいユーザーIDのリスト
    user_ids = [
        "111533746",  # WuBlockchain
        "963767159536209921",  # TheBlock__
        "1387497871751196672"   # WatcherGuru
    ]

    while True:
        for user_id in user_ids:
            await save_tweets_to_db(api, user_id, 1)  # 1はアカウント番号の例
        await asyncio.sleep(300)  # 5分ごとに実行

if __name__ == "__main__":
    asyncio.run(main())

# データベース接続を閉じる
conn.close()