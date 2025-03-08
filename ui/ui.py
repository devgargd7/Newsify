import time

import requests
import streamlit as st

RECOMMENDATION_API = "http://localhost:8001/recommendations/"
STORY_API = "http://localhost:8001/stories/"
TRACK_API = "http://localhost:8000/track"

def track_event(user_id, event_type, story_id=None, metadata=None):
    event_data = {
        "user_id": user_id,
        "event_type": event_type,
        "story_id": story_id,
        "event_time": time.time(),
        "metadata": metadata or {}
    }
    try:
        response = requests.post(TRACK_API, json=event_data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        st.error(f"Error tracking event: {e}")

def main():
    st.title("News Story Recommendation System")

    if "user_id" not in st.session_state:
        st.session_state.user_id = None

    if st.session_state.user_id is None:
        username = st.text_input("Enter your username:")
        if st.button("Login"):
            if username:
                st.session_state.user_id = username
                st.success(f"Logged in as {username}")
            else:
                st.error("Please enter a username")
    else:
        st.subheader("Recommended Stories")
        try:
            response = requests.get(f"{RECOMMENDATION_API}{st.session_state.user_id}")
            response.raise_for_status()
            stories = response.json()
            if not stories:
                st.warning("No recommendations available. Please try again later or check data.")
            else:
                for story in stories:
                    print(f"Story debug: _id={story.get('_id', 'missing')}, summary={story.get('summary', 'missing')}")
                    if st.button(story["summary"][:50] + "...", key=story["_id"]):
                        st.session_state.current_story = story["_id"]
                        track_event(st.session_state.user_id, "click", story["_id"])
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching recommendations: {e}. Please ensure the API is running.")

        if "current_story" in st.session_state:
            story_id = st.session_state.current_story
            try:
                response = requests.get(f"{STORY_API}{story_id}")
                response.raise_for_status()
                story = response.json()
                st.subheader("Story Details")
                st.write(f"**Summary:** {story['summary']}")
                # st.write(f"**Keywords:** {', '.join(story['keywords'])}")
                st.write(f"**Entities:** {', '.join(story['entities'])}")
                st.write("**Articles:**")
                for article in story["articles"]:
                    st.write(f"- {article}")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Like"):
                        track_event(st.session_state.user_id, "like", story_id)
                        st.success("Liked!")
                with col2:
                    if st.button("Share"):
                        share_url = f"http://localhost:8501/story/{story_id}"
                        st.write(f"Share this story: {share_url}")
                        track_event(st.session_state.user_id, "share", story_id)
                if "start_time" not in st.session_state:
                    st.session_state.start_time = time.time()
                if st.button("Back to Recommendations"):
                    time_spent = time.time() - st.session_state.start_time
                    track_event(st.session_state.user_id, "read", story_id, {"time_spent": time_spent})
                    del st.session_state.current_story
                    del st.session_state.start_time
            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching story details: {e}")

if __name__ == "__main__":
    main()