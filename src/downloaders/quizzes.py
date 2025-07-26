"""
Quizzes Downloader Module

This module implements the quizzes downloader for Canvas courses. Canvas quizzes
are assessments that can include various question types, timing restrictions,
multiple attempts, and detailed feedback. Quizzes are often graded and contribute
to course grades.

Canvas Quizzes include:
- Quiz settings and configuration
- Questions and question banks
- Answer choices and correct answers
- Timing and attempt restrictions
- Grading and point values
- Student submissions and attempts
- Feedback and explanations
- Quiz statistics and analytics

Features:
- Download quiz content and questions
- Export quiz settings and configuration
- Handle different question types (multiple choice, essay, etc.)
- Process quiz attempts and submissions (if accessible)
- Save quiz statistics and analytics
- Create printable quiz versions
- Handle quiz groups and question banks

Usage:
    # Initialize the downloader
    downloader = QuizzesDownloader(canvas_client, progress_tracker)

    # Download all quizzes for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import aiofiles
from canvasapi.quiz import Quiz
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class QuizzesDownloader(BaseDownloader):
    """
    Canvas Quizzes Downloader

    This class handles downloading quiz content from Canvas courses including
    quiz questions, settings, and associated metadata. It preserves quiz
    structure while making the content accessible for offline review.

    The downloader ensures that:
    - Complete quiz content is preserved
    - Question types and answers are documented
    - Quiz settings and restrictions are recorded
    - Feedback and explanations are included
    - Quiz statistics are captured (when available)
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the quizzes downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Quiz processing settings
        self.download_questions = True
        self.download_submissions = False  # Usually restricted
        self.include_answers = True  # May reveal correct answers
        self.download_statistics = True
        self.create_printable_version = True

        # Content processing options
        self.include_question_explanations = True
        self.process_question_banks = True
        self.include_quiz_groups = True

        # Security considerations
        self.respect_quiz_restrictions = True
        self.include_security_warning = True

        self.logger.info("Quizzes downloader initialized",
                         download_questions=self.download_questions,
                         include_answers=self.include_answers)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "quizzes"

    def fetch_content_list(self, course) -> List[Quiz]:
        """
        Fetch all quizzes from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Quiz]: List of quiz objects
        """
        try:
            self.logger.info(f"Fetching quizzes for course {course.id}")

            # Get quizzes with additional information
            quizzes = list(course.get_quizzes())

            self.logger.info(f"Found {len(quizzes)} quizzes",
                             course_id=course.id,
                             quiz_count=len(quizzes))

            return quizzes

        except CanvasException as e:
            self.logger.error(f"Failed to fetch quizzes", exception=e)
            raise DownloadError(f"Could not fetch quizzes: {e}")

    def extract_metadata(self, quiz: Quiz) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a quiz.

        Args:
            quiz: Canvas quiz object

        Returns:
            Dict[str, Any]: Quiz metadata
        """
        try:
            # Basic quiz information
            metadata = {
                'id': quiz.id,
                'title': quiz.title,
                'description': getattr(quiz, 'description', ''),
                'course_id': getattr(quiz, 'course_id', None),
                'assignment_id': getattr(quiz, 'assignment_id', None),
                'quiz_type': getattr(quiz, 'quiz_type', ''),
                'time_limit': getattr(quiz, 'time_limit', None),
                'shuffle_answers': getattr(quiz, 'shuffle_answers', False),
                'show_correct_answers': getattr(quiz, 'show_correct_answers', False),
                'show_correct_answers_last_attempt': getattr(quiz, 'show_correct_answers_last_attempt', False),
                'show_correct_answers_at': self._format_date(getattr(quiz, 'show_correct_answers_at', None)),
                'hide_correct_answers_at': self._format_date(getattr(quiz, 'hide_correct_answers_at', None)),
                'allowed_attempts': getattr(quiz, 'allowed_attempts', None),
                'scoring_policy': getattr(quiz, 'scoring_policy', ''),
                'one_question_at_a_time': getattr(quiz, 'one_question_at_a_time', False),
                'cant_go_back': getattr(quiz, 'cant_go_back', False),
                'access_code': getattr(quiz, 'access_code', ''),
                'ip_filter': getattr(quiz, 'ip_filter', ''),
                'anonymous_submissions': getattr(quiz, 'anonymous_submissions', False),
                'could_be_locked': getattr(quiz, 'could_be_locked', False),
                'locked_for_user': getattr(quiz, 'locked_for_user', False),
                'lock_explanation': getattr(quiz, 'lock_explanation', ''),
                'lock_info': getattr(quiz, 'lock_info', {}),
                'speed_grader_url': getattr(quiz, 'speed_grader_url', ''),
                'quiz_extensions_url': getattr(quiz, 'quiz_extensions_url', ''),
                'published': getattr(quiz, 'published', False),
                'unpublishable': getattr(quiz, 'unpublishable', True),
                'require_lockdown_browser': getattr(quiz, 'require_lockdown_browser', False),
                'require_lockdown_browser_for_results': getattr(quiz, 'require_lockdown_browser_for_results', False),
                'require_lockdown_browser_monitor': getattr(quiz, 'require_lockdown_browser_monitor', False),
                'lockdown_browser_monitor_data': getattr(quiz, 'lockdown_browser_monitor_data', ''),
                'question_count': getattr(quiz, 'question_count', 0),
                'points_possible': getattr(quiz, 'points_possible', None),
                'question_types': getattr(quiz, 'question_types', []),
                'has_access_code': bool(getattr(quiz, 'access_code', '')),
                'mobile_url': getattr(quiz, 'mobile_url', ''),
                'preview_url': getattr(quiz, 'preview_url', ''),
                'workflow_state': getattr(quiz, 'workflow_state', '')
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(quiz, 'created_at', None)),
                'updated_at': self._format_date(getattr(quiz, 'updated_at', None)),
                'due_at': self._format_date(getattr(quiz, 'due_at', None)),
                'lock_at': self._format_date(getattr(quiz, 'lock_at', None)),
                'unlock_at': self._format_date(getattr(quiz, 'unlock_at', None)),
            })

            # Assignment integration
            assignment_id = metadata.get('assignment_id')
            if assignment_id:
                metadata['is_graded'] = True
                metadata['assignment_integration'] = {
                    'assignment_id': assignment_id,
                    'note': 'This quiz is linked to a graded assignment'
                }
            else:
                metadata['is_graded'] = False

            # Security and access information
            security_features = []
            if metadata.get('require_lockdown_browser'):
                security_features.append('Lockdown Browser Required')
            if metadata.get('require_lockdown_browser_monitor'):
                security_features.append('Lockdown Browser Monitor Required')
            if metadata.get('access_code'):
                security_features.append('Access Code Required')
            if metadata.get('ip_filter'):
                security_features.append('IP Address Filtering')
            if metadata.get('time_limit'):
                security_features.append(f'Time Limit: {metadata["time_limit"]} minutes')

            metadata['security_features'] = security_features

            # Attempt and scoring information
            attempt_info = {
                'allowed_attempts': metadata.get('allowed_attempts', 'Unlimited'),
                'scoring_policy': metadata.get('scoring_policy', 'keep_highest'),
                'can_retake': metadata.get('allowed_attempts') != 1
            }
            metadata['attempt_information'] = attempt_info

            # Canvas URLs
            html_url = getattr(quiz, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract quiz metadata",
                              quiz_id=getattr(quiz, 'id', 'unknown'),
                              exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(quiz, 'id', None),
                'title': getattr(quiz, 'title', 'Unknown Quiz'),
                'description': '',
                'question_count': 0,
                'error': f"Metadata extraction failed: {e}"
            }

    def get_download_info(self, quiz: Quiz) -> Optional[Dict[str, str]]:
        """
        Get download information for a quiz.

        Quizzes are processed rather than directly downloaded.

        Args:
            quiz: Canvas quiz object

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all quizzes for a course.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting quizzes download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if quizzes are enabled
            if not self.config.is_content_type_enabled('quizzes'):
                self.logger.info("Quizzes download is disabled")
                return self.stats

            # Fetch quizzes
            quizzes = self.fetch_content_list(course)

            if not quizzes:
                self.logger.info("No quizzes found in course")
                return self.stats

            self.stats['total_items'] = len(quizzes)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(quizzes))

            # Process each quiz
            items_metadata = []

            for index, quiz in enumerate(quizzes, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(quiz)
                    metadata['item_number'] = index

                    # Process quiz content
                    await self._process_quiz(quiz, metadata, index)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process quiz",
                                      quiz_id=getattr(quiz, 'id', 'unknown'),
                                      quiz_title=getattr(quiz, 'title', 'unknown'),
                                      exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            self.logger.info(f"Quizzes download completed",
                             course_id=str(course.id),
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Quizzes download failed", exception=e)
            raise DownloadError(f"Quizzes download failed: {e}")

    async def _process_quiz(self, quiz: Quiz, metadata: Dict[str, Any], index: int):
        """
        Process a single quiz and create associated files.

        Args:
            quiz: Canvas quiz object
            metadata: Quiz metadata
            index: Quiz index number
        """
        try:
            quiz_title = self.sanitize_filename(quiz.title)

            # Create quiz-specific folder
            quiz_folder = self.content_folder / f"quiz_{index:03d}_{quiz_title}"
            quiz_folder.mkdir(exist_ok=True)

            # Save quiz overview
            await self._save_quiz_overview(quiz, metadata, quiz_folder, index)

            # Download quiz questions
            if self.download_questions and metadata.get('question_count', 0) > 0:
                await self._download_quiz_questions(quiz, metadata, quiz_folder)

            # Create printable version
            if self.create_printable_version:
                await self._create_printable_quiz(quiz, metadata, quiz_folder)

            # Download quiz statistics
            if self.download_statistics:
                await self._download_quiz_statistics(quiz, metadata, quiz_folder)

            # Save individual quiz metadata
            await self._save_quiz_metadata(metadata, quiz_folder)

        except Exception as e:
            self.logger.error(f"Failed to process quiz content",
                              quiz_id=quiz.id,
                              exception=e)
            raise

    async def _save_quiz_overview(self, quiz: Quiz, metadata: Dict[str, Any],
                                  quiz_folder: Path, index: int):
        """Save quiz overview and settings."""
        try:
            quiz_title = self.sanitize_filename(quiz.title)

            # Save as HTML
            html_filename = f"quiz_{index:03d}_{quiz_title}_overview.html"
            html_path = quiz_folder / html_filename

            html_content = self._create_quiz_html(quiz.title, metadata)

            async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved quiz overview as HTML",
                              file_path=str(html_path))

            # Save as text summary
            text_filename = f"quiz_{index:03d}_{quiz_title}_summary.txt"
            text_path = quiz_folder / text_filename

            text_content = self._create_quiz_text_summary(quiz.title, metadata)

            async with aiofiles.open(text_path, 'w', encoding='utf-8') as f:
                await f.write(text_content)

            self.logger.debug(f"Saved quiz summary as text",
                              file_path=str(text_path))

        except Exception as e:
            self.logger.error(f"Failed to save quiz overview", exception=e)

    def _create_quiz_html(self, quiz_title: str, metadata: Dict[str, Any]) -> str:
        """Create HTML overview of quiz content."""
        # Build quiz information
        info_html = ""

        if metadata.get('quiz_type'):
            info_html += f"<p><strong>Type:</strong> {metadata['quiz_type'].replace('_', ' ').title()}</p>"

        if metadata.get('points_possible'):
            info_html += f"<p><strong>Points:</strong> {metadata['points_possible']}</p>"

        if metadata.get('question_count'):
            info_html += f"<p><strong>Questions:</strong> {metadata['question_count']}</p>"

        if metadata.get('time_limit'):
            info_html += f"<p><strong>Time Limit:</strong> {metadata['time_limit']} minutes</p>"

        if metadata.get('allowed_attempts'):
            attempts = metadata['allowed_attempts']
            if attempts == -1:
                info_html += f"<p><strong>Attempts:</strong> Unlimited</p>"
            else:
                info_html += f"<p><strong>Attempts:</strong> {attempts}</p>"

        if metadata.get('due_at'):
            info_html += f"<p><strong>Due:</strong> {metadata['due_at']}</p>"

        if metadata.get('canvas_url'):
            info_html += f"<p><strong>Canvas URL:</strong> <a href=\"{metadata['canvas_url']}\">{metadata['canvas_url']}</a></p>"

        # Build security features
        security_html = ""
        if metadata.get('security_features'):
            security_html = "<h3>Security Features</h3><ul>"
            for feature in metadata['security_features']:
                security_html += f"<li>{feature}</li>"
            security_html += "</ul>"

        # Build description
        description_html = ""
        if metadata.get('description'):
            description_html = f"<h3>Description</h3><div>{metadata['description']}</div>"

        # Add security warning if needed
        warning_html = ""
        if self.include_security_warning and self.include_answers:
            warning_html = """
            <div class="security-warning">
                <h3>⚠️ Security Notice</h3>
                <p>This downloaded quiz content may include correct answers and sensitive information. 
                Please handle responsibly and in accordance with your institution's academic integrity policies.</p>
            </div>
            """

        # Create complete HTML document
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quiz: {quiz_title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .quiz-header {{
            background-color: #fff3e0;
            padding: 15px;
            border-left: 4px solid #ff9800;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .quiz-content {{
            margin-top: 20px;
        }}
        h1 {{
            color: #ff9800;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
        .quiz-badge {{
            background-color: #ff9800;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        .security-warning {{
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }}
        .security-warning h3 {{
            margin-top: 0;
            color: #856404;
        }}
    </style>
</head>
<body>
    <div class="quiz-badge">QUIZ</div>
    <h1>{quiz_title}</h1>

    {warning_html}

    <div class="quiz-header">
        <h2>Quiz Information</h2>
        {info_html}
    </div>

    <div class="quiz-content">
        {description_html}
        {security_html}
    </div>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    def _create_quiz_text_summary(self, quiz_title: str, metadata: Dict[str, Any]) -> str:
        """Create text summary of quiz content."""
        lines = [
            "=" * 60,
            f"QUIZ: {quiz_title}",
            "=" * 60,
            ""
        ]

        if self.include_security_warning and self.include_answers:
            lines.extend([
                "⚠️  SECURITY NOTICE:",
                "This content may include correct answers and sensitive information.",
                "Handle responsibly per academic integrity policies.",
                "",
                "-" * 60,
                ""
            ])

        # Basic information
        if metadata.get('quiz_type'):
            lines.append(f"Type: {metadata['quiz_type'].replace('_', ' ').title()}")

        if metadata.get('points_possible'):
            lines.append(f"Points: {metadata['points_possible']}")

        if metadata.get('question_count'):
            lines.append(f"Questions: {metadata['question_count']}")

        if metadata.get('time_limit'):
            lines.append(f"Time Limit: {metadata['time_limit']} minutes")

        if metadata.get('allowed_attempts'):
            attempts = metadata['allowed_attempts']
            if attempts == -1:
                lines.append("Attempts: Unlimited")
            else:
                lines.append(f"Attempts: {attempts}")

        if metadata.get('due_at'):
            lines.append(f"Due: {metadata['due_at']}")

        if metadata.get('canvas_url'):
            lines.append(f"Canvas URL: {metadata['canvas_url']}")

        lines.append("")

        # Security features
        if metadata.get('security_features'):
            lines.append("Security Features:")
            for feature in metadata['security_features']:
                lines.append(f"  - {feature}")
            lines.append("")

        # Description
        if metadata.get('description'):
            lines.append("Description:")
            # Simple HTML to text conversion for description
            description_text = metadata['description']
            # Remove HTML tags (basic)
            import re
            description_text = re.sub(r'<[^>]+>', '', description_text)
            lines.append(description_text.strip())
            lines.append("")

        # Footer
        lines.extend([
            "-" * 60,
            f"Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])

        return "\n".join(lines)

    async def _download_quiz_questions(self, quiz: Quiz, metadata: Dict[str, Any],
                                       quiz_folder: Path):
        """Download quiz questions and answers."""
        try:
            # Get quiz questions
            try:
                questions = list(quiz.get_questions())

                if not questions:
                    self.logger.info(f"No questions found for quiz {quiz.id}")
                    return

                # Create questions file
                questions_filename = "quiz_questions.json"
                questions_path = quiz_folder / questions_filename

                # Process questions
                questions_data = []
                for question in questions:
                    question_data = await self._process_quiz_question(question)
                    questions_data.append(question_data)

                # Create comprehensive questions document
                questions_document = {
                    'quiz_id': quiz.id,
                    'quiz_title': quiz.title,
                    'total_questions': len(questions_data),
                    'questions': questions_data,
                    'security_note': 'This content may include correct answers. Handle responsibly.',
                    'downloaded_at': datetime.now().isoformat()
                }

                async with aiofiles.open(questions_path, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(questions_document, indent=2, ensure_ascii=False, default=str))

                # Create human-readable questions file
                await self._create_readable_questions_file(questions_data, quiz, quiz_folder)

                self.logger.debug(f"Downloaded quiz questions",
                                  file_path=str(questions_path),
                                  question_count=len(questions_data))

            except Exception as e:
                self.logger.warning(f"Could not access quiz questions (may be restricted)",
                                    quiz_id=quiz.id,
                                    exception=e)

                # Create a note about restricted access
                note_path = quiz_folder / "questions_access_note.txt"
                note_content = f"""Quiz Questions Access Note

Quiz: {quiz.title}
Quiz ID: {quiz.id}

Quiz questions could not be downloaded. This may be due to:
- Quiz security settings
- Access restrictions
- API limitations
- Quiz not being published

To access questions, visit the quiz directly in Canvas:
{metadata.get('canvas_url', 'URL not available')}

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

                async with aiofiles.open(note_path, 'w', encoding='utf-8') as f:
                    await f.write(note_content)

        except Exception as e:
            self.logger.error(f"Failed to download quiz questions", exception=e)

    async def _process_quiz_question(self, question) -> Dict[str, Any]:
        """Process a single quiz question."""
        try:
            question_data = {
                'id': getattr(question, 'id', None),
                'question_name': getattr(question, 'question_name', ''),
                'question_text': getattr(question, 'question_text', ''),
                'question_type': getattr(question, 'question_type', ''),
                'position': getattr(question, 'position', None),
                'points_possible': getattr(question, 'points_possible', None),
                'correct_comments': getattr(question, 'correct_comments', ''),
                'incorrect_comments': getattr(question, 'incorrect_comments', ''),
                'neutral_comments': getattr(question, 'neutral_comments', ''),
                'more_comments': getattr(question, 'more_comments', ''),
                'text_after_answers': getattr(question, 'text_after_answers', ''),
                'answers': []
            }

            # Process question answers if available and enabled
            if self.include_answers:
                answers = getattr(question, 'answers', [])
                for answer in answers:
                    answer_data = {
                        'id': getattr(answer, 'id', None),
                        'text': getattr(answer, 'text', ''),
                        'weight': getattr(answer, 'weight', None),
                        'comments': getattr(answer, 'comments', ''),
                        'html': getattr(answer, 'html', ''),
                        'blank_id': getattr(answer, 'blank_id', None),
                        'answer_match_left': getattr(answer, 'answer_match_left', ''),
                        'answer_match_right': getattr(answer, 'answer_match_right', ''),
                        'matching_answer_incorrect_matches': getattr(answer, 'matching_answer_incorrect_matches', ''),
                        'numerical_answer_type': getattr(answer, 'numerical_answer_type', ''),
                        'exact': getattr(answer, 'exact', None),
                        'margin': getattr(answer, 'margin', None),
                        'approximate': getattr(answer, 'approximate', None),
                        'precision': getattr(answer, 'precision', None),
                        'start': getattr(answer, 'start', None),
                        'end': getattr(answer, 'end', None)
                    }

                    # Determine if this is a correct answer
                    weight = answer_data.get('weight', 0)
                    answer_data['is_correct'] = weight > 0

                    question_data['answers'].append(answer_data)

            return question_data

        except Exception as e:
            self.logger.warning(f"Failed to process quiz question",
                                question_id=getattr(question, 'id', 'unknown'),
                                exception=e)

            return {
                'id': getattr(question, 'id', None),
                'question_name': getattr(question, 'question_name', 'Unknown Question'),
                'question_text': '',
                'question_type': 'unknown',
                'error': f"Question processing failed: {e}"
            }

    async def _create_readable_questions_file(self, questions_data: List[Dict[str, Any]],
                                              quiz: Quiz, quiz_folder: Path):
        """Create human-readable questions file."""
        try:
            readable_filename = "quiz_questions_readable.html"
            readable_path = quiz_folder / readable_filename

            questions_html = ""

            for i, question in enumerate(questions_data, 1):
                question_html = f"""
                <div class="question">
                    <h3>Question {i}: {question.get('question_name', f'Question {i}')}</h3>
                    <p><strong>Type:</strong> {question.get('question_type', 'Unknown')}</p>
                    <p><strong>Points:</strong> {question.get('points_possible', 'Unknown')}</p>

                    <div class="question-text">
                        {question.get('question_text', 'No question text available')}
                    </div>
                """

                # Add answers if available
                answers = question.get('answers', [])
                if answers and self.include_answers:
                    question_html += "<div class='answers'><h4>Answer Choices:</h4><ol>"

                    for answer in answers:
                        answer_text = answer.get('text', answer.get('html', 'No answer text'))
                        is_correct = answer.get('is_correct', False)
                        correct_indicator = " ✓ CORRECT" if is_correct else ""

                        question_html += f"<li>{answer_text}{correct_indicator}</li>"

                    question_html += "</ol></div>"

                # Add feedback/comments
                comments = []
                if question.get('correct_comments'):
                    comments.append(f"<strong>Correct:</strong> {question['correct_comments']}")
                if question.get('incorrect_comments'):
                    comments.append(f"<strong>Incorrect:</strong> {question['incorrect_comments']}")
                if question.get('neutral_comments'):
                    comments.append(f"<strong>General:</strong> {question['neutral_comments']}")

                if comments:
                    question_html += "<div class='feedback'><h4>Feedback:</h4>"
                    question_html += "<br>".join(comments)
                    question_html += "</div>"

                question_html += "</div><hr>"
                questions_html += question_html

            # Create complete HTML document
            html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quiz Questions: {quiz.title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .question {{
            margin-bottom: 30px;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 8px;
            background-color: #f9f9f9;
        }}
        .question h3 {{
            color: #ff9800;
            margin-top: 0;
        }}
        .question-text {{
            background-color: white;
            padding: 15px;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .answers {{
            background-color: #f0f8ff;
            padding: 15px;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .feedback {{
            background-color: #f0fff0;
            padding: 15px;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .security-warning {{
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }}
    </style>
</head>
<body>
    <h1>Quiz Questions: {quiz.title}</h1>

    {'''<div class="security-warning">
        <strong>⚠️ Security Notice:</strong> This document contains quiz questions and correct answers. 
        Handle responsibly per academic integrity policies.
    </div>''' if self.include_security_warning and self.include_answers else ''}

    {questions_html}

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Total Questions: {len(questions_data)}</p>
    </footer>
</body>
</html>"""

            async with aiofiles.open(readable_path, 'w', encoding='utf-8') as f:
                await f.write(html_template)

            self.logger.debug(f"Created readable questions file",
                              file_path=str(readable_path))

        except Exception as e:
            self.logger.error(f"Failed to create readable questions file", exception=e)

    async def _create_printable_quiz(self, quiz: Quiz, metadata: Dict[str, Any],
                                     quiz_folder: Path):
        """Create a printable version of the quiz."""
        try:
            printable_filename = "quiz_printable.html"
            printable_path = quiz_folder / printable_filename

            # Note about printable version
            printable_note = """
            <div class="print-notice">
                <p><strong>Printable Quiz Version</strong></p>
                <p>This is a formatted version suitable for printing. 
                It includes quiz information and questions but may not include all interactive elements.</p>
            </div>
            """

            # Create simplified HTML for printing
            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Printable Quiz: {quiz.title}</title>
    <style>
        @media print {{
            body {{ margin: 0; font-size: 12pt; }}
            .no-print {{ display: none; }}
        }}
        body {{
            font-family: 'Times New Roman', serif;
            max-width: 100%;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.4;
        }}
        .print-notice {{
            background-color: #f0f8ff;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
        h1, h2, h3 {{ page-break-after: avoid; }}
        .question {{ 
            page-break-inside: avoid; 
            margin-bottom: 20px;
            padding: 10px;
            border: 1px solid #ccc;
        }}
    </style>
</head>
<body>
    <h1>{quiz.title}</h1>

    <div class="no-print">
        {printable_note}
    </div>

    <div class="quiz-info">
        <p><strong>Points:</strong> {metadata.get('points_possible', 'Not specified')}</p>
        <p><strong>Questions:</strong> {metadata.get('question_count', 'Unknown')}</p>
        {f"<p><strong>Time Limit:</strong> {metadata['time_limit']} minutes</p>" if metadata.get('time_limit') else ""}
        {f"<p><strong>Due:</strong> {metadata['due_at']}</p>" if metadata.get('due_at') else ""}
    </div>

    <hr>

    <p><em>Note: This is a downloaded version from Canvas. Questions and content may vary from the live quiz.</em></p>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

            async with aiofiles.open(printable_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Created printable quiz version",
                              file_path=str(printable_path))

        except Exception as e:
            self.logger.error(f"Failed to create printable quiz", exception=e)

    async def _download_quiz_statistics(self, quiz: Quiz, metadata: Dict[str, Any],
                                        quiz_folder: Path):
        """Download quiz statistics if available."""
        try:
            # Try to get quiz statistics
            try:
                statistics = quiz.get_statistics()

                if statistics:
                    stats_filename = "quiz_statistics.json"
                    stats_path = quiz_folder / stats_filename

                    # Process statistics
                    stats_data = {
                        'quiz_id': quiz.id,
                        'quiz_title': quiz.title,
                        'statistics': statistics,
                        'generated_at': datetime.now().isoformat()
                    }

                    async with aiofiles.open(stats_path, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(stats_data, indent=2, ensure_ascii=False, default=str))

                    self.logger.debug(f"Downloaded quiz statistics",
                                      file_path=str(stats_path))

            except Exception as e:
                self.logger.warning(f"Could not access quiz statistics",
                                    quiz_id=quiz.id,
                                    exception=e)

        except Exception as e:
            self.logger.error(f"Failed to download quiz statistics", exception=e)

    async def _save_quiz_metadata(self, metadata: Dict[str, Any], quiz_folder: Path):
        """Save individual quiz metadata."""
        try:
            metadata_path = quiz_folder / 'quiz_metadata.json'

            async with aiofiles.open(metadata_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            self.logger.debug(f"Saved quiz metadata",
                              file_path=str(metadata_path))

        except Exception as e:
            self.logger.error(f"Failed to save quiz metadata", exception=e)

    def _format_date(self, date_obj) -> str:
        """Format a date object to ISO string."""
        if not date_obj:
            return ""

        try:
            if hasattr(date_obj, 'isoformat'):
                return date_obj.isoformat()
            elif isinstance(date_obj, str):
                return date_obj
            else:
                return str(date_obj)
        except:
            return ""


# Register the downloader with the factory
from .base import ContentDownloaderFactory

ContentDownloaderFactory.register_downloader('quizzes', QuizzesDownloader)